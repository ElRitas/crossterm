use std::{collections::VecDeque, io, time::Duration};

use mio::{unix::SourceFd, Events, Interest, Poll, Token};
use signal_hook_mio::v1_0::Signals;

#[cfg(feature = "event-stream")]
use crate::event::sys::Waker;
use crate::event::{
    source::EventSource, sys::unix::parse::parse_event, timeout::PollTimeout, Event, InternalEvent,
};
use crate::terminal::sys::file_descriptor::{tty_fd, FileDesc};

// Tokens to identify file descriptor
const TTY_TOKEN: Token = Token(0);
const SIGNAL_TOKEN: Token = Token(1);
#[cfg(feature = "event-stream")]
const WAKE_TOKEN: Token = Token(2);

// I (@zrzka) wasn't able to read more than 1_022 bytes when testing
// reading on macOS/Linux -> we don't need bigger buffer and 1k of bytes
// is enough.
const TTY_BUFFER_SIZE: usize = 1_024;

/// Why the terminal input can not deliver events anymore.
#[derive(Clone, Copy, Debug)]
enum InputClosed {
    /// `read` returned 0: the other end of the terminal is gone.
    Eof,
    /// `read` failed with this OS error (`EIO` for a pty whose master side is gone).
    Os(i32),
    /// `read` failed with an error that carries no OS error code.
    Other(io::ErrorKind),
}

impl InputClosed {
    fn from_read_error(error: &io::Error) -> Self {
        match error.raw_os_error() {
            Some(code) => InputClosed::Os(code),
            None => InputClosed::Other(error.kind()),
        }
    }

    fn to_error(self) -> io::Error {
        match self {
            InputClosed::Eof => {
                io::Error::new(io::ErrorKind::UnexpectedEof, "terminal input closed")
            }
            InputClosed::Os(code) => io::Error::from_raw_os_error(code),
            InputClosed::Other(kind) => io::Error::new(kind, "terminal input failed"),
        }
    }
}

pub(crate) struct UnixInternalEventSource {
    poll: Poll,
    events: Events,
    parser: Parser,
    tty_buffer: [u8; TTY_BUFFER_SIZE],
    tty_fd: FileDesc<'static>,
    signals: Signals,
    /// Set once the terminal input is found closed or broken. `mio` reports readiness only on a
    /// change, so there will be no second wake-up for the same condition: report the stored error
    /// on every later `try_read` instead of waiting for input that can not arrive.
    closed: Option<InputClosed>,
    #[cfg(feature = "event-stream")]
    waker: Waker,
}

impl UnixInternalEventSource {
    pub fn new() -> io::Result<Self> {
        UnixInternalEventSource::from_file_descriptor(tty_fd()?)
    }

    pub(crate) fn from_file_descriptor(input_fd: FileDesc<'static>) -> io::Result<Self> {
        let poll = Poll::new()?;
        let registry = poll.registry();

        let tty_raw_fd = input_fd.raw_fd();
        let mut tty_ev = SourceFd(&tty_raw_fd);
        registry.register(&mut tty_ev, TTY_TOKEN, Interest::READABLE)?;

        let mut signals = Signals::new([signal_hook::consts::SIGWINCH])?;
        registry.register(&mut signals, SIGNAL_TOKEN, Interest::READABLE)?;

        #[cfg(feature = "event-stream")]
        let waker = Waker::new(registry, WAKE_TOKEN)?;

        Ok(UnixInternalEventSource {
            poll,
            events: Events::with_capacity(3),
            parser: Parser::default(),
            tty_buffer: [0u8; TTY_BUFFER_SIZE],
            tty_fd: input_fd,
            signals,
            closed: None,
            #[cfg(feature = "event-stream")]
            waker,
        })
    }
}

impl UnixInternalEventSource {
    /// Remembers that the input is closed. Events that were already parsed are delivered first;
    /// the error follows once they are consumed.
    fn report_closed(&mut self, closed: InputClosed) -> io::Result<Option<InternalEvent>> {
        self.closed = Some(closed);
        match self.parser.next() {
            Some(event) => Ok(Some(event)),
            None => Err(closed.to_error()),
        }
    }
}

impl EventSource for UnixInternalEventSource {
    fn try_read(&mut self, timeout: Option<Duration>) -> io::Result<Option<InternalEvent>> {
        if let Some(event) = self.parser.next() {
            return Ok(Some(event));
        }

        if let Some(closed) = self.closed {
            return Err(closed.to_error());
        }

        let timeout = PollTimeout::new(timeout);

        loop {
            if let Err(e) = self.poll.poll(&mut self.events, timeout.leftover()) {
                // Mio will throw an interrupted error in case of cursor position retrieval. We need to retry until it succeeds.
                // Previous versions of Mio (< 0.7) would automatically retry the poll call if it was interrupted (if EINTR was returned).
                // https://docs.rs/mio/0.7.0/mio/struct.Poll.html#notes
                if e.kind() == io::ErrorKind::Interrupted {
                    continue;
                } else {
                    return Err(e);
                }
            };

            if self.events.is_empty() {
                // No readiness events = timeout
                return Ok(None);
            }

            for token in self.events.iter().map(|x| x.token()) {
                match token {
                    TTY_TOKEN => {
                        loop {
                            match self.tty_fd.read(&mut self.tty_buffer) {
                                // End of input: the terminal went away. Spinning on it would never
                                // return control to the caller.
                                Ok(0) => return self.report_closed(InputClosed::Eof),
                                Ok(read_count) => {
                                    self.parser.advance(
                                        &self.tty_buffer[..read_count],
                                        read_count == TTY_BUFFER_SIZE,
                                    );
                                }
                                Err(e) => match e.kind() {
                                    // No more data to read at the moment. We will receive another
                                    // event once more data is available to read.
                                    io::ErrorKind::WouldBlock => break,
                                    io::ErrorKind::Interrupted => continue,
                                    // Anything else (e.g. `EIO` after the pty master was closed) is
                                    // not recoverable: hand it to the caller.
                                    _ => {
                                        return self
                                            .report_closed(InputClosed::from_read_error(&e));
                                    }
                                },
                            };

                            if let Some(event) = self.parser.next() {
                                return Ok(Some(event));
                            }
                        }
                    }
                    SIGNAL_TOKEN => {
                        if self.signals.pending().next() == Some(signal_hook::consts::SIGWINCH) {
                            // TODO Should we remove tput?
                            //
                            // This can take a really long time, because terminal::size can
                            // launch new process (tput) and then it parses its output. It's
                            // not a really long time from the absolute time point of view, but
                            // it's a really long time from the mio, async-std/tokio executor, ...
                            // point of view.
                            let new_size = crate::terminal::size()?;
                            return Ok(Some(InternalEvent::Event(Event::Resize(
                                new_size.0, new_size.1,
                            ))));
                        }
                    }
                    #[cfg(feature = "event-stream")]
                    WAKE_TOKEN => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::Interrupted,
                            "Poll operation was woken up by `Waker::wake`",
                        ));
                    }
                    _ => unreachable!("Synchronize Evented handle registration & token handling"),
                }
            }

            // Processing above can take some time, check if timeout expired
            if timeout.elapsed() {
                return Ok(None);
            }
        }
    }

    #[cfg(feature = "event-stream")]
    fn waker(&self) -> Waker {
        self.waker.clone()
    }
}

//
// Following `Parser` structure exists for two reasons:
//
//  * mimic anes Parser interface
//  * move the advancing, parsing, ... stuff out of the `try_read` method
//
#[derive(Debug)]
struct Parser {
    buffer: Vec<u8>,
    internal_events: VecDeque<InternalEvent>,
}

impl Default for Parser {
    fn default() -> Self {
        Parser {
            // This buffer is used for -> 1 <- ANSI escape sequence. Are we
            // aware of any ANSI escape sequence that is bigger? Can we make
            // it smaller?
            //
            // Probably not worth spending more time on this as "there's a plan"
            // to use the anes crate parser.
            buffer: Vec::with_capacity(256),
            // TTY_BUFFER_SIZE is 1_024 bytes. How many ANSI escape sequences can
            // fit? What is an average sequence length? Let's guess here
            // and say that the average ANSI escape sequence length is 8 bytes. Thus
            // the buffer size should be 1024/8=128 to avoid additional allocations
            // when processing large amounts of data.
            //
            // There's no need to make it bigger, because when you look at the `try_read`
            // method implementation, all events are consumed before the next TTY_BUFFER
            // is processed -> events pushed.
            internal_events: VecDeque::with_capacity(128),
        }
    }
}

impl Parser {
    fn advance(&mut self, buffer: &[u8], more: bool) {
        for (idx, byte) in buffer.iter().enumerate() {
            let more = idx + 1 < buffer.len() || more;

            self.buffer.push(*byte);

            match parse_event(&self.buffer, more) {
                Ok(Some(ie)) => {
                    self.internal_events.push_back(ie);
                    self.buffer.clear();
                }
                Ok(None) => {
                    // Event can't be parsed, because we don't have enough bytes for
                    // the current sequence. Keep the buffer and process next bytes.
                }
                Err(_) => {
                    // Event can't be parsed (not enough parameters, parameter is not a number, ...).
                    // Clear the buffer and continue with another sequence.
                    self.buffer.clear();
                }
            }
        }
    }
}

impl Iterator for Parser {
    type Item = InternalEvent;

    fn next(&mut self) -> Option<Self::Item> {
        self.internal_events.pop_front()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        ffi::OsStr,
        io::{self, Write},
        os::unix::{ffi::OsStrExt, net::UnixStream},
        sync::mpsc,
        thread,
        time::Duration,
    };

    use super::UnixInternalEventSource;
    use crate::event::source::EventSource;
    use crate::event::{Event, InternalEvent, KeyCode};
    use crate::terminal::sys::file_descriptor::FileDesc;

    /// How long a test waits for `try_read` before it decides the read loop never returns.
    const DEADLINE: Duration = Duration::from_secs(5);

    #[cfg(not(feature = "libc"))]
    fn file_desc(fd: impl Into<rustix::fd::OwnedFd>) -> FileDesc<'static> {
        FileDesc::Owned(fd.into())
    }

    #[cfg(feature = "libc")]
    fn file_desc(fd: impl std::os::unix::io::IntoRawFd) -> FileDesc<'static> {
        FileDesc::new(fd.into_raw_fd(), true)
    }

    /// Runs `f` on a helper thread. A reader that never returns (the bug these tests guard
    /// against) then fails the test instead of hanging the whole test run.
    fn run_with_deadline<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            let _ = tx.send(f());
        });
        rx.recv_timeout(DEADLINE)
            .expect("`try_read` did not return: the read loop spins on a closed terminal")
    }

    #[test]
    fn eof_on_input_is_reported_as_an_error() {
        let (input, peer) = UnixStream::pair().unwrap();
        drop(peer);

        let (first, second) = run_with_deadline(move || {
            let mut source =
                UnixInternalEventSource::from_file_descriptor(file_desc(input)).unwrap();
            let first = source.try_read(Some(Duration::from_secs(1)));
            let second = source.try_read(Some(Duration::from_secs(1)));
            (first, second)
        });

        // EOF is sticky: every later read reports it again instead of waiting forever.
        for result in [first, second] {
            let error = result.expect_err("EOF must be reported, not swallowed");
            assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
        }
    }

    #[test]
    fn input_is_delivered_before_the_closure_is_reported() {
        let (input, mut peer) = UnixStream::pair().unwrap();

        let (first, second) = run_with_deadline(move || {
            let mut source =
                UnixInternalEventSource::from_file_descriptor(file_desc(input)).unwrap();
            peer.write_all(b"a").unwrap();
            let first = source.try_read(Some(Duration::from_secs(1)));
            drop(peer);
            let second = source.try_read(Some(Duration::from_secs(1)));
            (first, second)
        });

        assert_eq!(
            first.unwrap(),
            Some(InternalEvent::Event(Event::Key(KeyCode::Char('a').into())))
        );
        let error = second.expect_err("EOF must follow the delivered input");
        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn closing_the_pty_master_is_reported_as_an_error() {
        use rustix::pty::{grantpt, openpt, ptsname, unlockpt, OpenptFlags};

        let master = openpt(OpenptFlags::RDWR | OpenptFlags::NOCTTY).unwrap();
        grantpt(&master).unwrap();
        unlockpt(&master).unwrap();
        let slave_name = ptsname(&master, Vec::new()).unwrap();
        let slave = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(OsStr::from_bytes(slave_name.to_bytes()))
            .unwrap();
        // This is what closing the terminal window does to the process reading the slave side.
        drop(master);

        let (first, second) = run_with_deadline(move || {
            let mut source =
                UnixInternalEventSource::from_file_descriptor(file_desc(slave)).unwrap();
            let first = source.try_read(Some(Duration::from_secs(1)));
            let second = source.try_read(Some(Duration::from_secs(1)));
            (first, second)
        });

        // Linux reports EIO for a pty slave whose master is gone; macOS reports plain EOF.
        for result in [first, second] {
            let error = result.expect_err("a closed terminal must be reported, not swallowed");
            assert!(
                error.kind() == io::ErrorKind::UnexpectedEof
                    || error.raw_os_error() == Some(rustix::io::Errno::IO.raw_os_error()),
                "unexpected error for a closed pty: {error:?}"
            );
        }
    }
}

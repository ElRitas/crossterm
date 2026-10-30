use std::{
    io,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{self, SyncSender},
        Arc,
    },
    task::{Context, Poll},
    thread,
    time::Duration,
};

use futures_core::stream::Stream;

use crate::event::{
    filter::EventFilter, lock_internal_event_reader, poll_internal, read_internal, sys::Waker,
    Event, InternalEvent,
};

/// A stream of `Result<Event>`.
///
/// **This type is not available by default. You have to use the `event-stream` feature flag
/// to make it available.**
///
/// It implements the [Stream](futures_core::stream::Stream)
/// trait and allows you to receive [`Event`]s with [`async-std`](https://crates.io/crates/async-std)
/// or [`tokio`](https://crates.io/crates/tokio) crates.
///
/// Check the [examples](https://github.com/crossterm-rs/crossterm/tree/master/examples) folder to see how to use
/// it (`event-stream-*`).
#[derive(Debug)]
pub struct EventStream {
    poll_internal_waker: Waker,
    stream_wake_task_executed: Arc<AtomicBool>,
    stream_wake_task_should_shutdown: Arc<AtomicBool>,
    task_sender: SyncSender<Task>,
    /// Set once the stream reported that the terminal input is closed: the stream then ends
    /// instead of repeating the same error on every poll.
    terminated: bool,
}

impl Default for EventStream {
    fn default() -> Self {
        let (task_sender, receiver) = mpsc::sync_channel::<Task>(1);

        thread::spawn(move || {
            while let Ok(task) = receiver.recv() {
                wait_for_input(
                    || poll_internal(None, &EventFilter),
                    &task.stream_wake_task_should_shutdown,
                );
                task.stream_wake_task_executed
                    .store(false, Ordering::SeqCst);
                task.stream_waker.wake();
            }
        });

        EventStream {
            poll_internal_waker: lock_internal_event_reader().waker(),
            stream_wake_task_executed: Arc::new(AtomicBool::new(false)),
            stream_wake_task_should_shutdown: Arc::new(AtomicBool::new(false)),
            task_sender,
            terminated: false,
        }
    }
}

impl EventStream {
    /// Constructs a new instance of `EventStream`.
    pub fn new() -> EventStream {
        EventStream::default()
    }
}

/// How [`wait_for_input`] ended.
#[derive(Debug, PartialEq, Eq)]
enum WaitOutcome {
    /// An event is ready to be read.
    InputAvailable,
    /// Polling failed, e.g. because the terminal was closed.
    PollFailed,
    /// The stream was dropped or asked to stop waiting.
    Shutdown,
}

/// Blocks until input is available, polling fails, or `should_shutdown` is set.
///
/// A polling error is not retried: such errors (the terminal is gone, the input reader could not
/// be initialized) do not go away, so retrying would spin this thread at 100% CPU and the stream
/// would never learn about them. Returning lets the caller wake the stream task, whose `poll_next`
/// polls again and hands the error to the consumer.
fn wait_for_input(
    mut poll: impl FnMut() -> io::Result<bool>,
    should_shutdown: &AtomicBool,
) -> WaitOutcome {
    loop {
        match poll() {
            Ok(true) => return WaitOutcome::InputAvailable,
            Ok(false) => {}
            Err(_) => return WaitOutcome::PollFailed,
        }

        if should_shutdown.load(Ordering::SeqCst) {
            return WaitOutcome::Shutdown;
        }
    }
}

/// Whether `error` means the terminal input is closed for good, so the stream can not continue.
fn input_closed(error: &io::Error) -> bool {
    if error.kind() == io::ErrorKind::UnexpectedEof {
        return true;
    }
    #[cfg(unix)]
    if error.raw_os_error() == Some(rustix::io::Errno::IO.raw_os_error()) {
        return true;
    }
    false
}

struct Task {
    stream_waker: std::task::Waker,
    stream_wake_task_executed: Arc<AtomicBool>,
    stream_wake_task_should_shutdown: Arc<AtomicBool>,
}

// Note to future me
//
// We need two wakers in order to implement EventStream correctly.
//
// 1. futures::Stream waker
//
// Stream::poll_next can return Poll::Pending which means that there's no
// event available. We are going to spawn a thread with the
// poll_internal(None, &EventFilter) call. This call blocks until an
// event is available and then we have to wake up the executor with notification
// that the task can be resumed.
//
// 2. poll_internal waker
//
// There's no event available, Poll::Pending was returned, stream waker thread
// is up and sitting in the poll_internal. User wants to drop the EventStream.
// We have to wake up the poll_internal (force it to return Ok(false)) and quit
// the thread before we drop.
impl Stream for EventStream {
    type Item = io::Result<Event>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.terminated {
            return Poll::Ready(None);
        }

        let result = match poll_internal(Some(Duration::from_secs(0)), &EventFilter) {
            Ok(true) => match read_internal(&EventFilter) {
                Ok(InternalEvent::Event(event)) => Poll::Ready(Some(Ok(event))),
                Err(e) => Poll::Ready(Some(Err(e))),
                #[cfg(unix)]
                _ => unreachable!(),
            },
            Ok(false) => {
                if !this
                    .stream_wake_task_executed
                    .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                    // https://github.com/rust-lang/rust/issues/80486#issuecomment-752244166
                    .unwrap_or_else(|x| x)
                {
                    let stream_waker = cx.waker().clone();
                    let stream_wake_task_executed = this.stream_wake_task_executed.clone();
                    let stream_wake_task_should_shutdown =
                        this.stream_wake_task_should_shutdown.clone();

                    stream_wake_task_should_shutdown.store(false, Ordering::SeqCst);

                    let _ = this.task_sender.send(Task {
                        stream_waker,
                        stream_wake_task_executed,
                        stream_wake_task_should_shutdown,
                    });
                }
                Poll::Pending
            }
            Err(e) => Poll::Ready(Some(Err(e))),
        };

        // Report a closed terminal once, then end the stream: the error is permanent, and a
        // consumer that keeps polling after it would otherwise loop on it.
        if let Poll::Ready(Some(Err(e))) = &result {
            if input_closed(e) {
                this.terminated = true;
            }
        }
        result
    }
}

impl Drop for EventStream {
    fn drop(&mut self) {
        self.stream_wake_task_should_shutdown
            .store(true, Ordering::SeqCst);
        let _ = self.poll_internal_waker.wake();
    }
}

#[cfg(test)]
mod tests {
    use std::{
        cell::Cell,
        io,
        sync::atomic::{AtomicBool, Ordering},
    };

    use super::{input_closed, wait_for_input, WaitOutcome};

    #[test]
    fn wait_for_input_returns_once_input_is_available() {
        let polls = Cell::new(0);
        let outcome = wait_for_input(
            || {
                polls.set(polls.get() + 1);
                Ok(polls.get() == 3)
            },
            &AtomicBool::new(false),
        );

        assert_eq!(outcome, WaitOutcome::InputAvailable);
        assert_eq!(polls.get(), 3);
    }

    #[test]
    fn wait_for_input_does_not_retry_after_a_poll_error() {
        let polls = Cell::new(0);
        let outcome = wait_for_input(
            || {
                polls.set(polls.get() + 1);
                Err(io::Error::new(io::ErrorKind::UnexpectedEof, "closed"))
            },
            &AtomicBool::new(false),
        );

        // One failed poll ends the wait. Before the fix the thread polled again and again,
        // spinning at 100% CPU and never telling the stream about the error.
        assert_eq!(outcome, WaitOutcome::PollFailed);
        assert_eq!(polls.get(), 1);
    }

    #[test]
    fn wait_for_input_stops_when_shutdown_is_requested() {
        let shutdown = AtomicBool::new(false);
        let outcome = wait_for_input(
            || {
                // The stream is dropped while the thread waits: `poll` is woken up and reports
                // that no input is available.
                shutdown.store(true, Ordering::SeqCst);
                Ok(false)
            },
            &shutdown,
        );

        assert_eq!(outcome, WaitOutcome::Shutdown);
    }

    #[test]
    fn only_a_closed_terminal_ends_the_stream() {
        assert!(input_closed(&io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "closed"
        )));
        #[cfg(unix)]
        assert!(input_closed(&io::Error::from_raw_os_error(
            rustix::io::Errno::IO.raw_os_error()
        )));

        assert!(!input_closed(&io::Error::new(
            io::ErrorKind::Other,
            "other"
        )));
        assert!(!input_closed(&io::Error::new(
            io::ErrorKind::Interrupted,
            "woken up"
        )));
    }
}

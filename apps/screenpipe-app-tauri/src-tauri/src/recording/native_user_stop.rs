// screenpipe — AI that knows everything you've seen, said, or heard
// https://screenpipe.com

use std::future::Future;
use tokio::sync::Mutex;

#[cfg(target_os = "macos")]
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
#[cfg(target_os = "macos")]
use tauri::{Emitter, Manager};
#[cfg(target_os = "macos")]
use tokio::sync::broadcast;

pub(super) trait UserStoppableCapture: Send {
    fn user_stop_id(&self) -> u64;
    fn stop_for_user(self) -> impl Future<Output = Result<(), String>> + Send;
}

pub(super) async fn stop_current_session<T, F, Fut>(
    session_id: u64,
    slot: &Mutex<Option<T>>,
    pause: F,
) -> Result<bool, String>
where
    T: UserStoppableCapture,
    F: FnOnce() -> Fut,
    Fut: Future<Output = ()>,
{
    let mut current = slot.lock().await;
    if current.as_ref().map(UserStoppableCapture::user_stop_id) != Some(session_id) {
        return Ok(false);
    }
    // Validate and pause under the same lock. A later Start cannot interleave
    // with teardown, and a late callback cannot pause a replacement session.
    pause().await;
    if let Some(session) = current.take() {
        session.stop_for_user().await?;
    }
    Ok(true)
}

#[cfg(target_os = "macos")]
static NEXT_USER_STOP_ID: AtomicU64 = AtomicU64::new(1);

#[cfg(target_os = "macos")]
pub(crate) struct UserStopMonitor {
    session_id: u64,
    subscription: screenpipe_screen::UserStopSubscription,
}

#[cfg(target_os = "macos")]
impl UserStopMonitor {
    pub(crate) fn new() -> Self {
        Self {
            session_id: NEXT_USER_STOP_ID.fetch_add(1, AtomicOrdering::Relaxed),
            subscription: screenpipe_screen::subscribe_user_stop(),
        }
    }

    pub(crate) fn session_id(&self) -> u64 {
        self.session_id
    }

    pub(crate) fn activate(self, app: tauri::AppHandle, mut shutdown: broadcast::Receiver<()>) {
        tokio::spawn(async move {
            let UserStopMonitor {
                session_id,
                mut subscription,
            } = self;
            let user_stopped = tokio::select! {
                _ = shutdown.recv() => false,
                _ = subscription.wait() => true,
            };
            if !user_stopped {
                return;
            }

            let state = app.state::<super::RecordingState>();
            match stop_current_session(session_id, &state.capture, || async {
                state.set_capture_intent(false);
                crate::health::set_recording_status(crate::health::RecordingStatus::Paused);
                if let Err(error) = app.emit("tray-recording-state-changed", "paused") {
                    tracing::warn!(%error, "failed to emit native user-stop paused state");
                }
                // Native Stop is a privacy control. Do not wait for the server
                // or query meeting history before releasing capture resources.
            })
            .await
            {
                Ok(true) => tracing::info!(session_id, "capture paused after native user Stop"),
                Ok(false) => tracing::debug!(session_id, "ignored stale native user Stop"),
                Err(error) => {
                    tracing::error!(session_id, %error, "failed to stop capture after native user Stop")
                }
            }
        });
    }
}

#[cfg(target_os = "macos")]
impl UserStoppableCapture for crate::capture_session::CaptureSession {
    fn user_stop_id(&self) -> u64 {
        self.user_stop_id
    }

    async fn stop_for_user(self) -> Result<(), String> {
        self.stop_checked().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    };

    struct Capture {
        id: u64,
        stops: Arc<AtomicUsize>,
    }

    impl UserStoppableCapture for Capture {
        fn user_stop_id(&self) -> u64 {
            self.id
        }

        async fn stop_for_user(self) -> Result<(), String> {
            self.stops.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn native_user_stop_pauses_and_stops_current_capture() {
        let intent = AtomicBool::new(true);
        let stops = Arc::new(AtomicUsize::new(0));
        let slot = Mutex::new(Some(Capture {
            id: 1,
            stops: stops.clone(),
        }));
        let stopped = stop_current_session(1, &slot, || async {
            intent.store(false, Ordering::SeqCst);
        })
        .await
        .unwrap();
        assert!(
            !intent.load(Ordering::SeqCst),
            "native user Stop must pause recording intent"
        );
        assert!(stopped);
        assert!(slot.lock().await.is_none());
        assert_eq!(stops.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn native_user_stop_cancels_pending_audio_start() {
        struct PendingAudioCapture(tokio::task::JoinHandle<()>);
        impl UserStoppableCapture for PendingAudioCapture {
            fn user_stop_id(&self) -> u64 {
                1
            }
            async fn stop_for_user(self) -> Result<(), String> {
                crate::capture_session::cancel_pending_audio_start(Some(self.0)).await;
                Ok(())
            }
        }

        let starts = Arc::new(AtomicUsize::new(0));
        let audio_starts = starts.clone();
        let pending = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            audio_starts.fetch_add(1, Ordering::SeqCst);
        });
        let slot = Mutex::new(Some(PendingAudioCapture(pending)));
        let intent = AtomicBool::new(true);
        assert!(stop_current_session(1, &slot, || async {
            intent.store(false, Ordering::SeqCst);
        })
        .await
        .unwrap());
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(!intent.load(Ordering::SeqCst));
        assert_eq!(
            starts.load(Ordering::SeqCst),
            0,
            "audio must not start after native user Stop"
        );
    }

    #[tokio::test]
    async fn stale_user_stop_token_does_not_pause_or_stop_replacement_capture() {
        let intent = AtomicBool::new(true);
        let stops = Arc::new(AtomicUsize::new(0));
        let slot = Mutex::new(Some(Capture {
            id: 2,
            stops: stops.clone(),
        }));

        let stopped = stop_current_session(1, &slot, || async {
            intent.store(false, Ordering::SeqCst);
        })
        .await
        .unwrap();

        assert!(!stopped);
        assert!(intent.load(Ordering::SeqCst));
        assert_eq!(
            slot.lock().await.as_ref().map(|capture| capture.id),
            Some(2)
        );
        assert_eq!(stops.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn duplicate_user_stop_does_not_pause_explicit_replacement_start() {
        let intent = AtomicBool::new(true);
        let stops = Arc::new(AtomicUsize::new(0));
        let slot = Mutex::new(Some(Capture {
            id: 1,
            stops: stops.clone(),
        }));

        assert!(stop_current_session(1, &slot, || async {
            intent.store(false, Ordering::SeqCst);
        })
        .await
        .unwrap());
        assert_eq!(stops.load(Ordering::SeqCst), 1);

        // An explicit Start installs a new owner and publishes recording intent.
        intent.store(true, Ordering::SeqCst);
        *slot.lock().await = Some(Capture {
            id: 2,
            stops: stops.clone(),
        });

        let duplicate = stop_current_session(1, &slot, || async {
            intent.store(false, Ordering::SeqCst);
        })
        .await
        .unwrap();

        assert!(!duplicate);
        assert!(intent.load(Ordering::SeqCst));
        assert_eq!(
            slot.lock().await.as_ref().map(|capture| capture.id),
            Some(2)
        );
        assert_eq!(stops.load(Ordering::SeqCst), 1);
    }
}

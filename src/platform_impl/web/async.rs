use atomic_waker::AtomicWaker;
use once_cell::unsync::Lazy;
use std::future::{self, Future};
use std::ops::Deref;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, RecvError, SendError, Sender, TryRecvError};
use std::sync::{Arc, Condvar, Mutex};
use std::task::{Context, Poll};
use wasm_bindgen::prelude::wasm_bindgen;
use wasm_bindgen::{JsCast, JsValue};

// Unsafe wrapper type that allows us to use `T` when it's not `Send` from other threads.
// `value` **must** only be accessed on the main thread.
pub struct MainThreadSafe<T: 'static, E: 'static> {
    value: Arc<Mutex<Option<T>>>,
    handler: fn(&mut T, E),
    sender: AsyncSender<E>,
    close: Flag,
}

impl<T, E> MainThreadSafe<T, E> {
    thread_local! {
        static MAIN_THREAD: Lazy<bool> = Lazy::new(|| {
            #[wasm_bindgen]
            extern "C" {
                #[derive(Clone)]
                pub(crate) type Global;

                #[wasm_bindgen(method, getter, js_name = Window)]
                fn window(this: &Global) -> JsValue;
            }

            let global: Global = js_sys::global().unchecked_into();
            !global.window().is_undefined()
        });
    }

    #[track_caller]
    pub fn new(value: T, handler: fn(&mut T, E)) -> Option<Self> {
        Self::MAIN_THREAD.with(|safe| {
            if !*safe.deref() {
                panic!("only callable from inside the `Window`")
            }
        });

        let value = Arc::new(Mutex::new(Some(value)));

        let (sender, receiver) = channel::<E>();
        let close = Flag::new();

        wasm_bindgen_futures::spawn_local({
            let value = value.clone();
            let mut close = close.clone();
            async move {
                while let Ok(event) = future::poll_fn(|cx| {
                    if let Poll::Ready(event) = Pin::new(&mut receiver.next()).poll(cx) {
                        return Poll::Ready(event);
                    }

                    if Pin::new(&mut close).poll(cx).is_ready() {
                        return Poll::Ready(Err(RecvError));
                    }

                    Poll::Pending
                })
                .await
                {
                    handler(value.lock().unwrap().as_mut().unwrap(), event)
                }

                value.lock().unwrap().take().unwrap();
            }
        });

        Some(Self {
            value,
            handler,
            sender,
            close,
        })
    }

    pub fn send(&self, event: E) {
        Self::MAIN_THREAD.with(|is_main_thread| {
            if *is_main_thread.deref() {
                (self.handler)(self.value.lock().unwrap().as_mut().unwrap(), event)
            } else {
                self.sender.send(event).unwrap()
            }
        })
    }

    fn is_main_thread(&self) -> bool {
        Self::MAIN_THREAD.with(|is_main_thread| *is_main_thread.deref())
    }

    pub fn with<R>(&self, f: impl FnOnce(&mut T) -> R) -> Option<R> {
        Self::MAIN_THREAD.with(|is_main_thread| {
            if *is_main_thread.deref() {
                Some(f(self.value.lock().unwrap().as_mut().unwrap()))
            } else {
                None
            }
        })
    }
}

impl<T, E> Clone for MainThreadSafe<T, E> {
    fn clone(&self) -> Self {
        Self {
            value: self.value.clone(),
            handler: self.handler,
            sender: self.sender.clone(),
            close: self.close.clone(),
        }
    }
}

impl<T, E> Drop for MainThreadSafe<T, E> {
    fn drop(&mut self) {
        if Arc::strong_count(&self.value) == 2 {
            self.close.signal();
        }
    }
}

unsafe impl<T, E> Send for MainThreadSafe<T, E> {}
unsafe impl<T, E> Sync for MainThreadSafe<T, E> {}

pub struct Dispatcher<T: 'static>(MainThreadSafe<T, Closure<T>>);

type Closure<T> = Box<dyn FnOnce(&mut T) + Send>;

impl<T> Dispatcher<T> {
    #[track_caller]
    pub fn new(value: T) -> Option<Self> {
        MainThreadSafe::new(value, |value, closure: Closure<T>| closure(value)).map(Self)
    }

    pub fn dispatch(&self, f: impl 'static + FnOnce(&mut T) + Send) {
        if self.is_main_thread() {
            self.with(|value| f(value)).unwrap()
        } else {
            self.send(Box::new(f))
        }
    }

    pub fn queue<R: 'static + Send>(&self, f: impl 'static + FnOnce(&mut T) -> R + Send) -> R {
        if self.is_main_thread() {
            self.with(|value| f(value)).unwrap()
        } else {
            let pair = Arc::new((Mutex::new(None), Condvar::new()));
            let closure: Closure<T> = Box::new({
                let pair = pair.clone();
                move |value| {
                    *pair.0.lock().unwrap() = Some(f(value));
                    pair.1.notify_one();
                }
            });

            self.send(closure);

            let mut started = pair.0.lock().unwrap();

            while started.is_none() {
                started = pair.1.wait(started).unwrap();
            }

            started.take().unwrap()
        }
    }
}

impl<T> Deref for Dispatcher<T> {
    type Target = MainThreadSafe<T, Box<dyn FnOnce(&mut T) + Send>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

fn channel<T>() -> (AsyncSender<T>, AsyncReceiver<T>) {
    let (sender, receiver) = mpsc::channel();
    let sender = Mutex::new(sender);
    let waker = Arc::new(AtomicWaker::new());

    let sender = AsyncSender {
        sender,
        waker: Arc::clone(&waker),
    };
    let receiver = AsyncReceiver { receiver, waker };

    (sender, receiver)
}

struct AsyncSender<T> {
    sender: Mutex<Sender<T>>,
    waker: Arc<AtomicWaker>,
}

impl<T> AsyncSender<T> {
    pub fn send(&self, event: T) -> Result<(), SendError<T>> {
        self.sender.lock().unwrap().send(event)?;
        self.waker.wake();

        Ok(())
    }
}

impl<T> Clone for AsyncSender<T> {
    fn clone(&self) -> Self {
        Self {
            sender: Mutex::new(self.sender.lock().unwrap().clone()),
            waker: self.waker.clone(),
        }
    }
}

struct AsyncReceiver<T> {
    receiver: Receiver<T>,
    waker: Arc<AtomicWaker>,
}

impl<T> AsyncReceiver<T> {
    pub fn next(&self) -> AsyncReceiverFuture<'_, T> {
        AsyncReceiverFuture(self)
    }
}

struct AsyncReceiverFuture<'a, T>(&'a AsyncReceiver<T>);

impl<T> Future for AsyncReceiverFuture<'_, T> {
    type Output = Result<T, RecvError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.0.receiver.try_recv() {
            Ok(event) => Poll::Ready(Ok(event)),
            Err(TryRecvError::Empty) => {
                self.0.waker.register(cx.waker());

                match self.0.receiver.try_recv() {
                    Ok(event) => Poll::Ready(Ok(event)),
                    Err(TryRecvError::Empty) => Poll::Pending,
                    Err(TryRecvError::Disconnected) => Poll::Ready(Err(RecvError)),
                }
            }
            Err(TryRecvError::Disconnected) => Poll::Ready(Err(RecvError)),
        }
    }
}

#[derive(Clone)]
struct Flag(Arc<Inner>);

struct Inner {
    waker: AtomicWaker,
    set: AtomicBool,
}

impl Flag {
    pub fn new() -> Self {
        Self(Arc::new(Inner {
            waker: AtomicWaker::new(),
            set: AtomicBool::new(false),
        }))
    }

    pub fn signal(&self) {
        self.0.set.store(true, Ordering::Relaxed);
        self.0.waker.wake();
    }
}

impl Future for Flag {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.0.set.load(Ordering::Relaxed) {
            return Poll::Ready(());
        }

        self.0.waker.register(cx.waker());

        if self.0.set.load(Ordering::Relaxed) {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

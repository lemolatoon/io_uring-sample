use std::collections::BTreeMap;
use std::io::{self, BufWriter, Write};
use std::os::unix::io::AsRawFd;
use std::sync::mpsc::Receiver;
use std::sync::{Arc, Mutex};

use io_uring::cqueue::Entry;
use io_uring::{IoUring, opcode};
use vmm_sys_util::{
    epoll::{ControlOperation, Epoll, EpollEvent, EventSet},
    eventfd::EventFd,
};

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

pub struct YieldNow {
    yielded: bool,
}

impl YieldNow {
    pub fn new() -> Self {
        Self { yielded: false }
    }
}

impl Future for YieldNow {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
        if self.yielded {
            Poll::Ready(())
        } else {
            self.yielded = true;
            Poll::Pending
        }
    }
}

async fn do_task<W: Write>(
    receiver: Arc<Mutex<Receiver<Entry>>>,
    ring: Arc<Mutex<IoUring>>,
    writer: Arc<Mutex<W>>,
    user_data: u64,
) {
    let e = opcode::Nop::new().build().user_data(user_data);
    {
        let mut ring = ring.lock().expect("lock");
        unsafe {
            ring.submission().push(&e).expect("push");
        }
        ring.submit().expect("submit");
    }

    println!("Task {} submitted! starting YieldNow", user_data);
    YieldNow::new().await;
    println!("Task {} back from yield!", user_data);

    let cqe = receiver.lock().expect("lock").recv().unwrap();
    assert_eq!(cqe.user_data(), user_data);

    writer
        .lock()
        .expect("lock")
        .write_fmt(format_args!(
            "Hello, world! user_data: {} -> {}, result: {}",
            user_data,
            cqe.user_data(),
            cqe.result()
        ))
        .expect("write");
}

pub fn kick_task(fut: &mut Pin<Box<impl Future<Output = ()>>>) {
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    match fut.as_mut().poll(&mut cx) {
        Poll::Ready(()) => panic!("Task should not be ready yet at first poll"),
        Poll::Pending => println!("Task is still pending"),
    }
}

pub fn complete_task(fut: &mut Pin<Box<impl Future<Output = ()>>>) {
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    match fut.as_mut().poll(&mut cx) {
        Poll::Ready(()) => println!("Task completed"),
        Poll::Pending => panic!("Task should be completed"),
    }
}

pub fn do_io() -> io::Result<()> {
    let ring = Arc::new(Mutex::new(IoUring::new(32)?));

    let eventfd = EventFd::new(0)?;
    let epoll = Epoll::new()?;
    epoll.ctl(
        ControlOperation::Add,
        eventfd.as_raw_fd(),
        EpollEvent::new(EventSet::IN, 3), // 0 for exit_event, 1, 2 for enqueue
    )?;
    ring.lock()
        .expect("lock")
        .submitter()
        .register_eventfd(eventfd.as_raw_fd())?;

    let mut buf = vec![0; 1024];
    let writer = Arc::new(Mutex::new(BufWriter::new(&mut buf)));
    let (sender, receiver) = std::sync::mpsc::channel::<Entry>();
    let receiver = Arc::new(Mutex::new(receiver));

    let mut task1 = Box::pin(do_task(
        Arc::clone(&receiver),
        Arc::clone(&ring),
        Arc::clone(&writer),
        1,
    ));
    kick_task(&mut task1);
    let mut task2 = Box::pin(do_task(
        Arc::clone(&receiver),
        Arc::clone(&ring),
        Arc::clone(&writer),
        2,
    ));
    kick_task(&mut task2);
    let mut task3 = Box::pin(do_task(
        Arc::clone(&receiver),
        Arc::clone(&ring),
        Arc::clone(&writer),
        3,
    ));
    kick_task(&mut task3);

    let mut task_map = BTreeMap::new();
    task_map.insert(1, task1);
    task_map.insert(2, task2);
    task_map.insert(3, task3);

    let mut epoll_events = vec![EpollEvent::new(EventSet::IN, 0); 10];

    let mut i = 0;
    let n_task = task_map.len();
    loop {
        let epoll_events_len = epoll.wait(-1, &mut epoll_events)?;
        eventfd.read()?;
        for event in epoll_events.iter().take(epoll_events_len) {
            println!("epoll event: {:?}", event);
            for cqe in ring.lock().expect("lock").completion() {
                let user_data = cqe.user_data();
                sender.send(cqe).expect("send");
                // enqueue_event
                let task = task_map.remove(&user_data);
                let mut task = match task {
                    Some(task) => task,
                    None => {
                        println!("No task found for event data: {}", event.data());
                        continue;
                    }
                };
                complete_task(&mut task);
                i += 1;
            }
        }
        if i >= n_task {
            break;
        }
    }
    drop(task_map);
    drop(writer);
    println!(
        "Contents of the buffer: {:?}",
        String::from_utf8_lossy(&buf)
    );

    Ok(())
}

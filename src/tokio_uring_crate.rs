use std::io;

use tokio::{io::unix::AsyncFd, task::JoinHandle};
use tokio_uring::{
    buf::{IoBuf, IoBufMut},
    fs::File,
};
use vmm_sys_util::eventfd::EventFd;

struct RawBuf {
    start: *mut u8,
    initialized: usize,
    len: usize,
}

impl RawBuf {
    fn new(size: usize) -> Self {
        let mut buf = vec![0; size];
        let capacity = buf.capacity();
        let start = buf.as_mut_ptr();
        std::mem::forget(buf);
        Self {
            start,
            initialized: 0,
            len: capacity,
        }
    }

    fn into_vec(self) -> Vec<u8> {
        unsafe { Vec::from_raw_parts(self.start, self.initialized, self.len) }
    }
}

unsafe impl IoBuf for RawBuf {
    fn stable_ptr(&self) -> *const u8 {
        self.start
    }

    fn bytes_init(&self) -> usize {
        self.initialized
    }

    fn bytes_total(&self) -> usize {
        self.len
    }
}

unsafe impl IoBufMut for RawBuf {
    fn stable_mut_ptr(&mut self) -> *mut u8 {
        self.start
    }

    unsafe fn set_init(&mut self, init_len: usize) {
        self.initialized = init_len;
    }
}

pub async fn await_on_eventfd(eventfd: &AsyncFd<EventFd>) -> io::Result<()> {
    println!("Waiting for eventfd...");
    eventfd
        .readable()
        .await?
        .try_io(|f| f.get_ref().read())
        .unwrap()?;
    Ok(())
}

#[allow(dead_code)]
async fn task() -> io::Result<()> {
    let eventfd = EventFd::new(0)?;
    let eventfd2 = eventfd.try_clone()?;
    let asyncfd = AsyncFd::new(eventfd)?;
    // Thread kicking eventfd when a line is read from stdin.
    std::thread::spawn(move || {
        let mut buf = String::new();
        loop {
            if let Err(err) = io::stdin().read_line(&mut buf) {
                println!("Error reading line: {}", err);
                continue;
            }
            buf.clear();
            eventfd2.write(1).unwrap();
        }
    });
    let mut tasks = Vec::new();
    await_on_eventfd(&asyncfd).await?;
    let tomltask: JoinHandle<io::Result<()>> = tokio_uring::spawn(async {
        let tomlfd = File::open("Cargo.toml").await?;
        let rawbuf = RawBuf::new(1024 * 8);
        let (result, buf) = tomlfd.read_at(rawbuf, 0).await;
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        match result {
            Ok(_) => {
                println!("Read {} bytes from Cargo.toml", buf.bytes_init());
                let v = buf.into_vec();
                println!(
                    "Cargo.toml first 10 chars:\n{}",
                    &String::from_utf8_lossy(&v)[..10]
                );
            }
            Err(e) => println!("Error reading Cargo.toml: {}", e),
        }

        Ok(())
    });
    tasks.push(tomltask);
    await_on_eventfd(&asyncfd).await?;
    let unicodetask = tokio_uring::spawn(async {
        let unicodefd = File::open("unicode_dump.txt").await?;
        let rawbuf = RawBuf::new(1024 * 8 * 8 * 8 * 2); // 2GiB
        let (result, buf) = unicodefd.read_at(rawbuf, 0).await;
        tokio::time::sleep(tokio::time::Duration::from_secs(10)).await; // emulate long read
        match result {
            Ok(_) => {
                println!("Read {} bytes from unicode_dump.txt", buf.bytes_init());
                let v = buf.into_vec();
                println!(
                    "unicode_dump.txt first 10 chars:\n{}",
                    &String::from_utf8_lossy(&v)[..10]
                );
            }
            Err(e) => println!("Error reading unicode_dump.txt: {}", e),
        }
        Ok(())
    });
    tasks.push(unicodetask);

    tokio_uring::fs::create_dir_all("tmp").await?;
    for i in 0..10 {
        await_on_eventfd(&asyncfd).await?;
        tasks.push(tokio_uring::spawn(async move {
            let fd = File::create(format!("tmp/{}.txt", i)).await?;
            println!("Created tmp/{}.txt", i);
            let content = format!(
                "Hello! Writing {}th file now!! This file is really small, should be done quickly.",
                i
            )
            .into_bytes();
            let (result, buf) = fd.write_all_at(content, i).await;
            match result {
                Ok(_) => println!("Wrote {} bytes to tmp/{}.txt", buf.bytes_init(), i),
                Err(e) => println!("Error writing to tmp/{}.txt: {}", i, e),
            }

            Ok(())
        }));
    }
    futures::future::join_all(tasks).await;
    let mut tasks: Vec<JoinHandle<io::Result<()>>> = Vec::new();
    for i in 0..10 {
        await_on_eventfd(&asyncfd).await?;
        tasks.push(tokio_uring::spawn(async move {
            let fd = File::open(format!("tmp/{}.txt", i)).await?;
            let rawbuf = RawBuf::new(1024);
            let (result, buf) = fd.read_at(rawbuf, 0).await;
            match result {
                Ok(_) => {
                    println!("Read {} bytes from tmp/{}.txt", buf.bytes_init(), i);
                    let v = buf.into_vec();
                    println!(
                        "tmp/{}.txt first 10 chars:\n{}",
                        i,
                        &String::from_utf8_lossy(&v)[..10]
                    );
                }
                Err(e) => println!("Error reading tmp/{}.txt: {}", i, e),
            }
            Ok(())
        }));
    }

    futures::future::join_all(tasks).await;
    Ok(())
}

#[allow(dead_code)]
pub fn do_io() -> io::Result<()> {
    tokio_uring::builder().entries(512).start(task()).unwrap();
    Ok(())
}

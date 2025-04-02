use std::{io, vec};

use tokio::{
    fs::File,
    io::{AsyncReadExt as _, AsyncWriteExt, unix::AsyncFd},
    task::JoinHandle,
};
use vmm_sys_util::eventfd::EventFd;

use crate::tokio_uring_crate::await_on_eventfd;

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
    let tomltask: JoinHandle<io::Result<()>> = tokio::spawn(async {
        println!("Cargo.toml task started");
        let mut tomlfd = match File::open("Cargo.toml").await {
            Ok(fd) => fd,
            Err(err) => {
                println!("Error opening Cargo.toml: {}", err);
                return Err(err);
            }
        };
        let mut buf = vec![0; 1024];
        let n = match tomlfd.read(&mut buf).await {
            Ok(n) => n,
            Err(err) => {
                println!("Error reading Cargo.toml: {}", err);
                return Err(err);
            }
        };
        println!("Read {} bytes from Cargo.toml", n);
        println!(
            "Cargo.toml first 10 chars:\n{}",
            &String::from_utf8_lossy(&buf[..n])[..10]
        );

        Ok(())
    });
    tasks.push(tomltask);
    await_on_eventfd(&asyncfd).await?;
    let unicodetask = tokio::spawn(async {
        println!("unicode_dump.txt task started");
        let mut unicodefd = File::open("unicode_dump.txt").await?;
        let mut buf = vec![0; 1024 * 8 * 8 * 8 * 2]; // 2GiB
        let n = unicodefd.read(&mut buf).await?;
        tokio::time::sleep(tokio::time::Duration::from_secs(10)).await; // emulate long-lasting read
        println!("Read {} bytes from unicode_dump.txt", n);
        println!(
            "unicode_dump.txt first 10 chars:\n{}",
            &String::from_utf8_lossy(&buf[..n])[..10]
        );
        Ok(())
    });
    tasks.push(unicodetask);

    tokio::fs::create_dir_all("tmp").await?;
    for i in 0..10 {
        await_on_eventfd(&asyncfd).await?;
        tasks.push(tokio::spawn(async move {
            println!("Creating tmp/{}.txt", i);
            let mut fd = File::create(format!("tmp/{}.txt", i)).await?;
            println!("Created tmp/{}.txt", i);
            let content = format!(
                "Hello! Writing {}th file now!! This file is really small, should be done quickly.",
                i
            )
            .into_bytes();
            fd.write_all(&content[..]).await?;
            println!("Wrote {} bytes to tmp/{}.txt", content.len(), i);

            Ok(())
        }));
    }
    futures::future::join_all(tasks).await;
    let mut tasks: Vec<JoinHandle<io::Result<()>>> = Vec::new();
    for i in 0..10 {
        await_on_eventfd(&asyncfd).await?;
        tasks.push(tokio::spawn(async move {
            println!("Reading tmp/{}.txt", i);
            let mut fd = File::open(format!("tmp/{}.txt", i)).await?;
            let mut buf = vec![0; 1024];
            let n = fd.read(&mut buf).await?;
            println!("Read {} bytes from tmp/{}.txt", n, i);
            println!(
                "tmp/{}.txt first 10 chars:\n{}",
                i,
                &String::from_utf8_lossy(&buf[..n])[..10]
            );
            Ok(())
        }));
    }

    futures::future::join_all(tasks).await;
    Ok(())
}

#[allow(dead_code)]
pub fn do_io() -> io::Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()?;
    runtime.block_on(task())
}

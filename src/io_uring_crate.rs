use std::{
    ffi::CStr,
    fs::{self, File},
    io,
    os::fd::AsRawFd as _,
};

use io_uring::{IoUring, opcode, types};
use vmm_sys_util::{
    epoll::{ControlOperation, Epoll, EpollEvent, EventSet},
    eventfd::EventFd,
};

pub fn do_io() -> io::Result<()> {
    let mut ring = IoUring::new(512)?;
    let eventfd = EventFd::new(0)?;
    let epoll = Epoll::new()?;
    epoll.ctl(
        ControlOperation::Add,
        eventfd.as_raw_fd(),
        EpollEvent::new(EventSet::IN, 3), // 0 for exit_event, 1, 2 for enqueue
    )?;
    ring.submitter()
        .register_eventfd_async(eventfd.as_raw_fd())?;

    let mut bufs = Vec::new();
    let mut fds = Vec::new();
    for _ in 0..20 {
        bufs.push(Vec::new());
    }
    let mut entries = vec![];
    let mut buf_idx = 0;
    let tomlfd = fs::File::open("Cargo.toml")?;
    {
        bufs[buf_idx].resize(1024, 0);
        let read_e = opcode::Read::new(
            types::Fd(tomlfd.as_raw_fd()),
            bufs[buf_idx].as_mut_ptr(),
            bufs[buf_idx].len() as _,
        )
        .build()
        .user_data(buf_idx as u64); // index to bufs
        entries.push(read_e);
    }
    buf_idx += 1;

    let unicodefd = fs::File::open("unicode_dump.txt")?;
    {
        bufs[buf_idx].resize(1024 * 8 * 8 * 8 * 2, 0); // 2GiB
        let read_e = opcode::Read::new(
            types::Fd(unicodefd.as_raw_fd()),
            bufs[buf_idx].as_mut_ptr(),
            bufs[buf_idx].len() as _,
        )
        .build()
        .user_data(buf_idx as u64);
        entries.push(read_e);
    }
    buf_idx += 1;

    fs::create_dir_all("tmp")?;
    for i in 0..10 {
        let fd = File::create(format!("tmp/{}.txt", i)).unwrap();
        fds.push(fd);
    }
    for (i, fd) in fds.iter().enumerate() {
        bufs[buf_idx] = format!(
            "Hello! Writing {}th file now!! This file is really small, should be done quickly.",
            i
        )
        .as_bytes()
        .to_vec();
        let write_e = opcode::Write::new(
            types::Fd(fd.as_raw_fd()),
            bufs[buf_idx].as_mut_ptr(),
            bufs[buf_idx].len() as _,
        )
        .build()
        .user_data(buf_idx as u64);
        entries.push(write_e);
        buf_idx += 1;
    }

    for e in entries {
        unsafe {
            ring.submission()
                .push(&e)
                .expect("submission queue is full");
        }
    }
    ring.submit()?;

    // ring.submit_and_wait(n_entry)?;
    const EPOLL_EVENTS_LEN: usize = 100;
    let mut events = vec![EpollEvent::new(EventSet::empty(), 0); EPOLL_EVENTS_LEN];

    loop {
        let num_events = epoll.wait(-1, &mut events[..])?;
        eventfd.read()?; // read kick FD
        println!("num_events: {}", num_events);
        for event in events.iter().take(num_events) {
            let evset = match EventSet::from_bits(event.events) {
                Some(evset) => evset,
                None => {
                    let evbits = event.events;
                    println!("epoll: ignoring unknown event set: 0x{:x}", evbits);
                    continue;
                }
            };

            let ev_type = event.data() as u16;

            // handle_event() returns true if an event is received from the exit event fd.
            println!("evset: {:?}, ev_type: {}", evset, ev_type);
            for cqe in ring.completion() {
                println!("Completed at {}, result: {}", cqe.user_data(), cqe.result());
                if cqe.user_data() == 1 {
                    println!("peak the content.");
                    let buf = unsafe { CStr::from_ptr(bufs[1].as_ptr().cast::<i8>()) };
                    let str = buf.to_str().unwrap().to_string();
                    println!("First 10/{} chars:\n", str.len());
                    for c in str.chars().take(10) {
                        print!("{c}");
                    }
                }
            }
        }
    }

    // Ok(())
}

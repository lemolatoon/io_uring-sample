use std::io;

mod io_uring_crate;
mod io_uring_crate_with_future;
mod tokio_crate;
mod tokio_uring_crate;

fn main() -> io::Result<()> {
    // io_uring_crate::do_io()?;
    io_uring_crate_with_future::do_io()?;
    // tokio_uring_crate::do_io()?;
    // tokio_crate::do_io()?;

    Ok(())
}

use std::io;

mod io_uring_crate;
mod tokio_crate;
mod tokio_uring_crate;

fn main() -> io::Result<()> {
    // io_uring_crate::do_io()?;
    // tokio_uring_crate::do_io()?;
    tokio_crate::do_io()?;

    Ok(())
}

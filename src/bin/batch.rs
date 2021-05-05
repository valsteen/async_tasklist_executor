use async_std::channel::unbounded;
use async_std::fs::{File, OpenOptions};
use async_std::task;
use clap::{App, Arg};
use log::LevelFilter;

use async_tasklist_executor::{Void, TaskParameter};
use async_tasklist_executor::process_entry::process_entry;

fn main() -> Result<(), String> {
    env_logger::builder()
        .filter_level(LevelFilter::Info)
        .format_timestamp_secs()
        .init();

    let arg_matches = App::new("Vendor portal user creation")
        .arg(
            Arg::with_name("input")
                .takes_value(true)
                .long("input")
                .required(true),
        )
        .arg(
            Arg::with_name("output")
                .takes_value(true)
                .long("output")
                .required(true),
        )
        .arg(
            Arg::with_name("workers")
                .takes_value(true)
                .long("workers")
                .required(false)
                .default_value("4"),
        )
        .get_matches();

    let workers: usize = arg_matches
        .value_of("workers")
        .unwrap()
        .parse()
        .expect("Numerical value expected");

    let csv_reader = {
        let input_filename = arg_matches.value_of("input").unwrap();
        let input_file = task::block_on(File::open(input_filename))
            .map_err(|e| format!("Cannot open file {} for reading: {}", input_filename, e))?;
        csv_async::AsyncReaderBuilder::new()
            .has_headers(false)
            .flexible(true)
            .delimiter(b';')
            .create_reader(input_file)
    };

    let csv_writer = {
        let output_filename = arg_matches.value_of("output").unwrap();
        let output_file = task::block_on(
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(output_filename),
        )
        .map_err(|e| format!("Cannot open file {} for writing: {}", output_filename, e))?;
        csv_async::AsyncWriterBuilder::new()
            .has_headers(false)
            .delimiter(b';')
            .create_writer(output_file)
    };

    let (task_sender, task_receiver) = unbounded();
    let (result_sender, result_receiver) = unbounded();
    let (shutdown_sender, shutdown_receiver) = unbounded::<Void>();
    ctrlc::set_handler(move || {
        shutdown_sender.close();
    })
    .expect("Error setting Ctrl-C handler");

    // this way we can initialize a 'task processor' that can setup some state that can then
    // be read/change at every iteration
    let future_factory = |worker_id: String| async move {
        // demonstrates a modifiable state between calls
        let mut n_request = 0;
        move |parameter: TaskParameter| {
            n_request += 1;
            process_entry(format!("Worker {} Request {}", worker_id, n_request),
                          parameter)
        }
    };

    async_tasklist_executor::prepare_workers(
        workers,
        task_receiver,
        result_sender,
        shutdown_receiver,
        future_factory,
    );
    async_tasklist_executor::process_loop(csv_reader, csv_writer, task_sender, result_receiver);

    Ok(())
}

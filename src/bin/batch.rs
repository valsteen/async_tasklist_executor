use async_std::channel::{bounded, unbounded};
use async_std::fs::{File, OpenOptions};
use async_std::task;
use clap::{App, Arg};
use futures::future::join_all;
use log::LevelFilter;

use async_tasklist_executor::{csv_read_loop, TaskRow};
use async_tasklist_executor::process_entry::process_entry;

struct ProcessState {
    request_count: usize,
    worker_name: String,
}

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
        .arg(
            Arg::with_name("retries")
                .takes_value(true)
                .long("retries")
                .required(false)
                .default_value("5"),
        )
        .get_matches();

    let workers: usize = arg_matches
        .value_of("workers")
        .unwrap()
        .parse()
        .expect("Numerical value expected");

    let retries: usize = arg_matches
        .value_of("retries")
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
            .flexible(true)
            .create_writer(output_file)
    };

    // TODO in order to not load all csv in memory, task_sender/receiver channel should be bounded,
    // but we then need a non-bounded retry channel, or workers may block at retrying.
    let (task_sender, task_receiver) = unbounded();
    let (result_sender, result_receiver) = bounded(workers);
    {
        let task_sender = task_sender.clone();
        ctrlc::set_handler(move || {
            task_sender.close();
        })
    }

        .expect("Error setting Ctrl-C handler");

    // this way we can initialize a 'task processor' that can setup some state that can then
    // be read/change at every iteration
    let future_factory = |worker_id: String| async move {
        // demonstrates a modifiable state between calls
        let mut state = ProcessState { request_count: 0, worker_name: worker_id };
        move |parameter: TaskRow<String>| {
            state.request_count += 1;
            process_entry(format!("{} Request {}", state.worker_name, state.request_count),
                          parameter)
        }
    };

    task::block_on(async {
        let workers_handle = task::spawn(async_tasklist_executor::prepare_workers(
            workers,
            task_receiver,
            task_sender.clone(),
            result_sender.clone(),
            future_factory,
            retries
        ));
        let csv_reader_handle = task::spawn(csv_read_loop(
            csv_reader,
            task_sender.clone(),
            result_sender,
        ));

        // ends once all tasks have been written out
        async_tasklist_executor::write_loop(csv_writer, result_receiver).await;

        // this stops workers
        task_sender.close();

        join_all(vec![workers_handle, csv_reader_handle]).await;
    });

    Ok(())
}

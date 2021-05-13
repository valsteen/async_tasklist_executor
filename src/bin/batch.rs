use clap::{App, Arg};
use log::LevelFilter;

use async_tasklist_executor::example_process_entry::process_entry;
use async_tasklist_executor::tasklist_executor::{TaskListExecutor, TaskRow};

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

    // this way we can initialize a 'task processor' that can setup some state that can then
    // be read/change at every iteration
    let future_factory = |worker_id: String| async move {
        // demonstrates a modifiable state between calls
        let mut state = ProcessState {
            request_count: 0,
            worker_name: worker_id,
        };
        move |parameter: TaskRow<String>| {
            state.request_count += 1;
            process_entry(
                format!("{} Request {}", state.worker_name, state.request_count),
                parameter,
            )
        }
    };

    TaskListExecutor::start(
        arg_matches.value_of("input").unwrap().to_string(),
        arg_matches.value_of("output").unwrap().to_string(),
        future_factory,
        workers,
        retries,
    )?;

    Ok(())
}

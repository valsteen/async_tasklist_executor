use clap::{App, Arg};
use log::LevelFilter;

use async_std::sync::Mutex;
use async_tasklist_executor::csv::{csv_stream, CsvWriter, InputData};
use async_tasklist_executor::example_process_entry::process_entry;
use async_tasklist_executor::tasklist_executor::{
    PinnedTaskPayloadStream, TaskListExecutor, TaskPayload, TaskPayloadStreamFactoryType,
    TaskProcessor, TaskProcessorFactory, TaskResult,
};
use futures::future::BoxFuture;
use std::sync::atomic::{AtomicI32, Ordering};

struct ProcessState {
    request_count: usize,
    worker_name: String,
}

struct ProcessorFactory {
    count: AtomicI32,
}

impl TaskProcessorFactory for ProcessorFactory {
    type Data = InputData;
    type Processor = Processor;

    fn new_task_processor(&self) -> BoxFuture<Processor> {
        let n = self.count.load(Ordering::Acquire) + 1;
        self.count.store(n, Ordering::Release);
        let id = format!("Worker {}", n);

        Box::pin(async { Processor::new(id) })
    }
}

struct Processor {
    state: Mutex<ProcessState>,
}

impl Processor {
    fn new(id: String) -> Self {
        Self {
            state: Mutex::new(ProcessState {
                request_count: 0,
                worker_name: id,
            }),
        }
    }
}

impl TaskProcessor for Processor {
    type Data = InputData;

    fn process_task(
        &self,
        task_payload: TaskPayload<Self::Data>,
    ) -> BoxFuture<'_, TaskResult<Self::Data>> {
        Box::pin(async move {
            let worker_id = {
                let mut state = self.state.lock().await;
                state.request_count += 1;
                format!("{} Request {}", state.worker_name, state.request_count)
            };
            process_entry(worker_id, task_payload).await
        })
    }
}

struct CsvInputStreamFactory {
    filename: String,
}

impl TaskPayloadStreamFactoryType for CsvInputStreamFactory {
    type Data = InputData;

    fn get_stream(self) -> BoxFuture<'static, Result<PinnedTaskPayloadStream<Self::Data>, String>> {
        Box::pin(csv_stream(self.filename))
    }
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

    let row_processor_factory = ProcessorFactory {
        count: Default::default(),
    };

    let input_filename = arg_matches.value_of("input").unwrap().to_string();
    let input_factory = CsvInputStreamFactory {
        filename: input_filename,
    };

    let csv_writer = CsvWriter::new(arg_matches.value_of("output").unwrap().to_string())?;

    TaskListExecutor::start(
        input_factory,
        csv_writer,
        row_processor_factory,
        workers,
        retries,
    )?;

    Ok(())
}

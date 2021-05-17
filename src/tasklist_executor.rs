use std::fmt::{Debug, Display, Formatter};
use std::marker::PhantomData;

use async_std::channel::{bounded, Receiver, Sender};
use async_std::future::Future;
use async_std::stream::Stream;
use async_std::sync::Arc;
use async_std::task;
use chrono::Local;
use futures::future::{join_all, BoxFuture};
use futures::StreamExt;
use log::{error, info};

#[derive(Clone, Debug)]
pub struct TaskPayload<Data: TaskData> {
    pub line: usize,
    pub data: Data,
    pub success_ts: Option<String>,
    pub error: Option<String>,
    pub attempt: usize,
}

#[derive(Clone)]
pub enum TaskError {
    ProcessingError(String),
    Shutdown,
}

impl Display for TaskError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(
            &match self {
                TaskError::ProcessingError(msg) => msg.clone(),
                TaskError::Shutdown => "Shutdown".to_string(),
            },
            f,
        )
    }
}

#[derive(Clone)]
pub enum TaskResult<Data: TaskData> {
    Success(TaskPayload<Data>),
    Error(TaskPayload<Data>, TaskError),
    Retry(TaskPayload<Data>, String),
}

pub trait TaskData: Clone + Debug + Send + Sync + Display + 'static {}

pub trait TaskProcessorFactory: Send + Sync + 'static {
    type Data: TaskData;
    type Processor: TaskProcessor<Data = Self::Data>;

    fn new_task_processor(&self) -> BoxFuture<Self::Processor>;
}

pub trait TaskProcessor: Send + Sync {
    type Data: TaskData;
    fn process_task(
        &self,
        task_payload: TaskPayload<Self::Data>,
    ) -> BoxFuture<TaskResult<Self::Data>>;
}

async fn write_loop<Data: TaskData, Writer, WriterResult>(
    mut write_record: Writer,
    result_receiver: Receiver<WriterMsg<Data>>,
) where
    Writer: FnMut(TaskPayload<Data>) -> WriterResult,
    WriterResult: Future<Output = Result<(), String>> + Unpin,
{
    let mut read_count = None;
    let mut write_count = 0;

    while let Ok(msg) = result_receiver.recv().await {
        let task_payload = match msg {
            WriterMsg::Payload(task_payload) => task_payload,
            WriterMsg::Eof(s) => {
                read_count = Some(s);

                assert!(write_count <= s);

                if write_count >= s {
                    info!("all records written out, leaving");
                    return;
                }

                continue;
            }
        };

        if let Err(e) = write_record(task_payload.clone()).await {
            error!(
                "Error while writing back {:?} ({}), quitting",
                task_payload, e
            );
            return;
        };

        write_count += 1;

        if let Some(read_count) = read_count {
            assert!(write_count <= read_count);
            if write_count >= read_count {
                info!("all records written out, leaving");
                return;
            }
        }
    }
}

enum WriterMsg<Data: TaskData> {
    Payload(TaskPayload<Data>),
    Eof(usize),
}

pub trait RecordWriter {
    type DataType: TaskData;
    fn write_record(
        &self,
        task_payload: TaskPayload<Self::DataType>,
    ) -> BoxFuture<'static, Result<(), String>>;
}

pub struct TaskListExecutor<
    Data,
    ProcessorFactory,
    TaskPayloadStream,
    TaskPayloadStreamFactory,
    RecordWriterType,
> {
    // Type parameters are declared at struct level in order to state the constraints only once.
    // But rust requires a usage at that point, so this phantom data is a 0-sized field which
    // has dummy references to the declared types.
    #[allow(clippy::type_complexity)]
    phantom_data: PhantomData<(
        Data,
        ProcessorFactory,
        TaskPayloadStream,
        TaskPayloadStreamFactory,
        RecordWriterType,
    )>,
}

impl<
        Data,
        ProcessorFactory,
        TaskPayloadStream,
        TaskPayloadStreamFactory,
        RecordWriterType,
    >
    TaskListExecutor<
        Data,
        ProcessorFactory,
        TaskPayloadStream,
        TaskPayloadStreamFactory,
        RecordWriterType,
    >
where
    Data: TaskData,
    ProcessorFactory: TaskProcessorFactory<Data = Data>,
    TaskPayloadStream: Stream<Item = TaskPayload<Data>> + Unpin + Send + 'static,
    TaskPayloadStreamFactory:
        Future<Output = Result<TaskPayloadStream, String>> + Unpin + Send + 'static,
    RecordWriterType: RecordWriter<DataType = Data> + 'static
{
    async fn read_loop(
        reader_factory: TaskPayloadStreamFactory,
        task_sender: Sender<TaskPayload<Data>>,
        result_sender: Sender<WriterMsg<Data>>,
    ) {
        let mut tasks_count: usize = 0;

        let mut reader = match reader_factory.await {
            Ok(reader) => reader,
            Err(err) => {
                error!("{}, leaving reader", err);
                return;
            }
        };
        while let Some(mut task_payload) = reader.next().await {
            tasks_count += 1;

            if task_payload.success_ts.is_some() {
                if let Err(e) = result_sender
                    .send(WriterMsg::Payload(task_payload.clone()))
                    .await
                {
                    error!("cannot write back {:?} ({}), quitting", task_payload, e);
                    return;
                }
                continue;
            } else if let Err(e) = task_sender.send(task_payload.clone()).await {
                error!(
                    "cannot send task, writing back {:?} ( {} )",
                    task_payload, e
                );
                task_payload.error = Some("Shutdown".to_string());
                if let Err(e) = result_sender.send(WriterMsg::Payload(task_payload)).await {
                    error!("cannot write back unprocessed ({}), quitting", e);
                    return;
                };
            };
        }

        if let Err(e) = result_sender.send(WriterMsg::Eof(tasks_count)).await {
            error!("cannot send eof after {} tasks ( {} )", tasks_count, e);
        }
    }

    async fn prepare_workers(
        workers_count: usize,
        task_receiver: Receiver<TaskPayload<Data>>,
        task_sender: Sender<TaskPayload<Data>>,
        result_sender: Sender<WriterMsg<Data>>,
        process_entry_factory: ProcessorFactory,
        retries: usize,
    ) {
        let mut workers = vec![];
        let process_entry_factory = Arc::new(process_entry_factory);

        for n in 0..workers_count {
            let task_receiver = task_receiver.clone();
            let task_sender = task_sender.clone();
            let result_sender = result_sender.clone();
            let process_entry_factory = process_entry_factory.clone();

            workers.push(task::spawn(async move {
                let entry_processor = process_entry_factory.new_task_processor().await;

                while let Ok(task_row) = task_receiver.recv().await {
                    let task_result = if !task_sender.is_closed() {
                        entry_processor.process_task(task_row.clone()).await
                    } else {
                        TaskResult::Error(task_row, TaskError::Shutdown)
                    };

                    let error_task_row = match task_result {
                        TaskResult::Success(mut task_row) => {
                            task_row.success_ts = Some(Local::now().to_rfc3339());
                            task_row.error = None;
                            if let Err(e) = result_sender.send(WriterMsg::Payload(task_row.clone())).await {
                                error!("Could not output successful record {:?} : {}, quitting", task_row, e);
                                return;
                            }
                            continue;
                        }
                        TaskResult::Error(mut task_row, task_error) => {
                            task_row.success_ts = None;
                            let error_msg = if let TaskError::Shutdown = task_error {
                                if task_row.error.is_some() {
                                    if task_row.attempt > 0 {
                                        format!("Shutdown before retrying ( last error: {} )", task_row.error.unwrap())
                                    } else {
                                        format!("Shutdown before processing ( original error: {} )", task_row.error.unwrap())
                                    }
                                } else {
                                    "Shutdown before processing".to_string()
                                }
                            } else {
                                task_error.to_string()
                            };
                            task_row.error = Some(error_msg);
                            task_row
                        }
                        TaskResult::Retry(mut task_row, task_error) => {
                            task_row.error = Some(task_error.to_string());
                            task_row.attempt += 1;
                            if task_row.attempt <= retries {
                                // since task sender is bounded we may block at writing, while all
                                // other workers are blocked at writing. in order to stay bounded
                                // which serves as a rate limit of memory usage by blocking the csv
                                // reader, here the hack is to emulate an unbounded retry channel by
                                // spawning a task that pushes to the channel
                                let task_sender = task_sender.clone();
                                let result_sender = result_sender.clone();

                                task::spawn(async move {
                                    if let Err(e) = task_sender.send(task_row.clone()).await {
                                        error!("Could not send {:?} for retry ( {} )", task_row, e);
                                        task_row.error = Some(format!("Shutdown before retrying ( last error: {} )", task_row.error.unwrap()));
                                        if let Err(e) = result_sender.send(WriterMsg::Payload(task_row.clone())).await {
                                            error!("Could not output failed record {:?} : dropping it ( {} )", task_row, e);
                                        }
                                    }
                                });
                                continue;
                            }
                            task_row
                        }
                    };

                    if let Err(e) = result_sender.send(WriterMsg::Payload(error_task_row.clone())).await {
                        error!("Could not output failed record {:?} : {}, quitting", error_task_row, e);
                        return;
                    }
                }

                info!("{}: graceful exit", n);
            }));
        }

        join_all(workers).await;
    }

    pub fn start(
        input_stream_factory: TaskPayloadStreamFactory,
        record_writer: RecordWriterType,
        process_row_factory: ProcessorFactory,
        workers_count: usize,
        retries: usize,
    ) -> Result<(), String> {
        let (task_sender, task_receiver) = bounded(workers_count);
        let (result_sender, result_receiver) = bounded(workers_count);
        {
            let task_sender = task_sender.clone();
            let result_sender = result_sender.clone();
            let mut counter = 0;

            ctrlc::set_handler(move || {
                counter += 1;
                if counter == 1 {
                    task_sender.close();
                } else {
                    result_sender.close();
                }
            })
        }
        .expect("Error setting Ctrl-C handler");

        task::block_on(async {
            let record_writer = Arc::new(record_writer);

            let workers_handle = task::spawn(Self::prepare_workers(
                workers_count,
                task_receiver,
                task_sender.clone(),
                result_sender.clone(),
                process_row_factory,
                retries,
            ));
            let csv_reader_handle = task::spawn({
                let task_sender = task_sender.clone();
                async move { Self::read_loop(input_stream_factory, task_sender, result_sender).await }
            });

            let writer = move |task_row| record_writer.write_record(task_row);

            // ends once all tasks have been written out
            write_loop(writer, result_receiver).await;

            // this stops workers
            task_sender.close();

            join_all(vec![workers_handle, csv_reader_handle]).await;
        });

        Ok(())
    }
}

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
pub struct TaskRow<Data>
where
    Data: Clone,
{
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
pub enum TaskResult<Data: Clone> {
    Success(TaskRow<Data>),
    Error(TaskRow<Data>, TaskError),
    Retry(TaskRow<Data>, String),
}

async fn write_loop<Data, Writer, WriterResult>(
    mut write_record: Writer,
    result_receiver: Receiver<WriterMsg<Data>>,
) where
    Data: Display + Debug + Clone,
    Writer: FnMut(TaskRow<Data>) -> WriterResult,
    WriterResult: Future<Output = Result<(), String>> + Unpin,
{
    let mut read_count = None;
    let mut write_count = 0;

    while let Ok(msg) = result_receiver.recv().await {
        let task_row = match msg {
            WriterMsg::TaskRow(task_row) => task_row,
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

        if let Err(e) = write_record(task_row.clone()).await {
            error!("Error while writing back {:?} ({}), quitting", task_row, e);
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

enum WriterMsg<Data: Clone> {
    TaskRow(TaskRow<Data>),
    Eof(usize),
}

pub struct TaskListExecutor<
    Data,
    FutureProcessor,
    FutureFactoryResult,
    FutureFactory,
    FutureResult,
    TaskRowStream,
    RecordWriterType,
> {
    data: PhantomData<Data>,
    data1: PhantomData<FutureProcessor>,
    data2: PhantomData<FutureFactoryResult>,
    data3: PhantomData<FutureFactory>,
    data4: PhantomData<FutureResult>,
    data5: PhantomData<TaskRowStream>,
    data6: PhantomData<RecordWriterType>,
}

pub trait RecordWriter {
    type DataType: Clone;
    fn write_record(
        self: &Arc<Self>,
        task_row: TaskRow<Self::DataType>,
    ) -> BoxFuture<'static, Result<(), String>>;
}

impl<
        Data,
        FutureProcessor,
        FutureFactoryResult,
        FutureFactory,
        FutureResult,
        TaskRowStream,
        RecordWriterType,
    >
    TaskListExecutor<
        Data,
        FutureProcessor,
        FutureFactoryResult,
        FutureFactory,
        FutureResult,
        TaskRowStream,
        RecordWriterType,
    >
where
    Data: Clone + Debug + Send + Sync + Display + 'static,
    FutureProcessor: FnMut(TaskRow<Data>) -> FutureResult + Send + 'static,
    FutureFactoryResult: Future<Output = FutureProcessor> + Send + 'static,
    FutureFactory: Fn(String) -> FutureFactoryResult + Clone + Send + Sync + 'static,
    FutureResult: Future<Output = TaskResult<Data>> + Send + 'static,
    TaskRowStream: Stream<Item = TaskRow<Data>> + Unpin + Send + 'static,
    RecordWriterType: RecordWriter<DataType = Data> + 'static,
{
    async fn read_loop(
        mut reader: TaskRowStream,
        task_sender: Sender<TaskRow<Data>>,
        result_sender: Sender<WriterMsg<Data>>,
    ) {
        let mut tasks_count: usize = 0;

        while let Some(mut task_row) = reader.next().await {
            tasks_count += 1;

            if task_row.success_ts.is_some() {
                if let Err(e) = result_sender
                    .send(WriterMsg::TaskRow(task_row.clone()))
                    .await
                {
                    error!("cannot write back {:?} ({}), quitting", task_row, e);
                    return;
                }
                continue;
            } else if let Err(e) = task_sender.send(task_row.clone()).await {
                error!("cannot send task, writing back {:?} ( {} )", task_row, e);
                task_row.error = Some("Shutdown".to_string());
                if let Err(e) = result_sender.send(WriterMsg::TaskRow(task_row)).await {
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
        task_receiver: Receiver<TaskRow<Data>>,
        task_sender: Sender<TaskRow<Data>>,
        result_sender: Sender<WriterMsg<Data>>,
        process_entry_factory: FutureFactory,
        retries: usize,
    ) {
        let mut workers = vec![];
        for n in 0..workers_count {
            let task_receiver = task_receiver.clone();
            let task_sender = task_sender.clone();
            let result_sender = result_sender.clone();
            let process_entry_factory = process_entry_factory.clone();

            workers.push(task::spawn(async move {
                let mut process_entry = process_entry_factory(format!("Worker {}", n)).await;

                while let Ok(task_row) = task_receiver.recv().await {
                    let task_result = if !task_sender.is_closed() {
                        process_entry(task_row.clone()).await
                    } else {
                        TaskResult::Error(task_row, TaskError::Shutdown)
                    };

                    let error_task_row = match task_result {
                        TaskResult::Success(mut task_row) => {
                            task_row.success_ts = Some(Local::now().to_rfc3339());
                            task_row.error = None;
                            if let Err(e) = result_sender.send(WriterMsg::TaskRow(task_row.clone())).await {
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
                                        if let Err(e) = result_sender.send(WriterMsg::TaskRow(task_row.clone())).await {
                                            error!("Could not output failed record {:?} : dropping it ( {} )", task_row, e);
                                        }
                                    }
                                });
                                continue;
                            }
                            task_row
                        }
                    };

                    if let Err(e) = result_sender.send(WriterMsg::TaskRow(error_task_row.clone())).await {
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
        input_stream: TaskRowStream,
        record_writer: RecordWriterType,
        future_factory: FutureFactory,
        workers_count: usize,
        retries: usize,
    ) -> Result<(), String> {
        let (task_sender, task_receiver) = bounded(workers_count);
        let (result_sender, result_receiver) = bounded(workers_count);
        {
            let task_sender = task_sender.clone();
            ctrlc::set_handler(move || {
                task_sender.close();
            })
        }
        .expect("Error setting Ctrl-C handler");

        let record_writer = Arc::new(record_writer);

        task::block_on(async {
            let workers_handle = task::spawn(Self::prepare_workers(
                workers_count,
                task_receiver,
                task_sender.clone(),
                result_sender.clone(),
                future_factory,
                retries,
            ));
            let csv_reader_handle = task::spawn({
                let task_sender = task_sender.clone();
                async move { Self::read_loop(input_stream, task_sender, result_sender).await }
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

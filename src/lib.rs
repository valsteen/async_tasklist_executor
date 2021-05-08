use std::fmt::{Debug, Display, Formatter};

use async_std::channel::{Receiver, Sender};
use async_std::fs::File;
use async_std::future::Future;
use async_std::task;
use chrono::Local;
use csv_async::{AsyncReader, AsyncWriter, StringRecord};
use futures::future::join_all;
use futures::StreamExt;
use log::{error, info};

pub mod process_entry;

#[derive(Clone, Debug)]
pub struct TaskRow<Data> where Data : Clone {
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
        std::fmt::Debug::fmt(&match self {
            TaskError::ProcessingError(msg) => msg.clone(),
            TaskError::Shutdown => "shutdown before completion".to_string(),
        }, f)
    }
}


#[derive(Clone)]
pub enum TaskResult<Data: Clone> {
    Success(TaskRow<Data>),
    Error(TaskRow<Data>, TaskError),
    Retry(TaskRow<Data>, String),
}


pub async fn write_loop<Data>(
    mut csv_writer: AsyncWriter<File>,
    result_receiver: Receiver<WriterMsg<Data>>,
) where Data : Display + Clone {

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
                    return ;
                }

                continue;
            }
        };

        let record = match task_row.success_ts {
            None => {
                StringRecord::from(vec![
                    task_row.data.to_string(),
                    "".to_string(),
                    task_row.error.unwrap_or_default()
                ])
            }
            Some(ts) => {
                StringRecord::from(vec![
                    task_row.data.to_string(),
                    ts
                ])
            }
        };

        if let Err(e) = csv_writer.write_record(&record).await {
            error!("Error while writing back {:?} ({}), quitting", record, e);
            return;
        }

        write_count += 1;

        if let Some(read_count) = read_count {
            assert!(write_count <= read_count);
            if write_count >= read_count {
                info!("all records written out, leaving");
                return ;
            }
        }
    }
}


pub async fn make_task_row<Data: Clone>(record: StringRecord, line_number: usize) -> Result<TaskRow<Data>, String>
where Data : From<String>
{
    let url = match record.get(0) {
        None => {
            info!("line {}: skipping empty line", line_number);
            return Err("empty line".to_string());
        }
        Some(url) => url.to_string(),
    };

    let success_ts = match record.get(1) {
        None => None,
        Some(ts) => {
            if ts.is_empty() {
                None
            } else {
                Some(ts.to_string())
            }
        }
    };

    Ok(TaskRow {
        line: line_number,
        data: url.into(),
        success_ts,
        error: None,
        attempt: 0,
    })
}

pub enum WriterMsg<Data: Clone> {
    TaskRow(TaskRow<Data>),
    Eof(usize)
}

pub async fn csv_read_loop<Data: Clone + From<String> + Debug>(
    mut csv_reader: AsyncReader<File>,
    task_sender: Sender<TaskRow<Data>>,
    result_sender: Sender<WriterMsg<Data>>,
) {
    let mut line_number: usize = 0;
    let mut tasks_count: usize = 0;

    while let Some(record_result) = csv_reader.records().next().await {
        line_number += 1;

        let line_result = match record_result {
            Ok(record) => {
                make_task_row(record, line_number).await
            }
            Err(err) => {
                error!("line {}: skipping record ({})", line_number, err);
                continue;
            }
        };

        let mut task_row = match line_result {
            Ok(task_row) => task_row,
            Err(_) => {
                continue;
            }
        };

        tasks_count += 1 ;

        if task_row.success_ts.is_some() {
            if let Err(e) = result_sender.send( WriterMsg::TaskRow(task_row.clone())).await {
                error!("cannot write back {:?} ({}), quitting", task_row, e);
                return;
            }
            continue;
        } else if let Err(e) = task_sender.send(task_row.clone()).await {
            error!("cannot send task, writing back {:?} ( {} )", task_row, e);
            task_row.error = Some("Shutdown".to_string());
            if let Err(e) = result_sender.send( WriterMsg::TaskRow(task_row)).await {
                error!("cannot write back unprocessed ({}), quitting", e);
                return;
            };
        };
    }

    if let Err(e) = result_sender.send(WriterMsg::Eof(tasks_count)).await {
        error!("cannot send eof after {} tasks ( {} )", tasks_count, e);
    }
}


pub async fn prepare_workers<Data: Clone + Debug + Send + Sync + 'static, FutureFactory, FutureFactoryResult, FutureProcessor, FutureResult>(
    workers_count: usize,
    task_receiver: Receiver<TaskRow<Data>>,
    task_sender: Sender<TaskRow<Data>>,
    result_sender: Sender<WriterMsg<Data>>,
    process_entry_factory: FutureFactory,
    retries: usize,
) where
    FutureFactory: Fn(String) -> FutureFactoryResult + Clone + Send + Sync + 'static,
    FutureFactoryResult: Future<Output=FutureProcessor> + Send,
    FutureProcessor: FnMut(TaskRow<Data>) -> FutureResult + Send,
    FutureResult: Future<Output=TaskResult<Data>> + Send
{
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
                        if let Err(e) = result_sender.send( WriterMsg::TaskRow(task_row.clone())).await {
                            error!("Could not output successful record {:?} : {}, quitting", task_row, e);
                            return;
                        }
                        continue;
                    }
                    TaskResult::Error(mut task_row, task_error) => {
                        task_row.success_ts = None;
                        task_row.error = Some(task_error.to_string());
                        task_row
                    }
                    TaskResult::Retry(mut task_row, task_error) => {
                        task_row.attempt += 1;
                        if task_row.attempt <= retries {
                            match task_sender.send(task_row.clone()).await {
                                Ok(_) => {
                                    continue;
                                }
                                Err(e) => {
                                    error!("Could not send {:?} for retry ( {} )", task_row, e);
                                    // let task_row go through the error processing
                                }
                            }
                        }
                        task_row.error = Some(task_error.to_string());
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

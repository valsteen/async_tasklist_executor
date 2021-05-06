use std::collections::{HashMap, HashSet};

use async_std::channel::{Receiver, Sender};
use async_std::fs::File;
use async_std::future::Future;
use async_std::task;
use chrono::{DateTime, Local};
use csv_async::{AsyncReader, AsyncWriter};
use futures::future::join_all;
use futures::StreamExt;
use log::{error, info};

pub mod process_entry;

pub enum Void {}

#[derive(Clone, Debug)]
pub enum ProcessError {
    Unrecoverable(String),
    Retry(String),
}

#[derive(Clone, Debug)]
pub struct TaskParameter {
    pub line: usize,
    pub url: String,
}

pub type TaskResult = Result<DateTime<Local>, ProcessError>;

#[derive(Clone)]
pub struct CompletedTask {
    pub parameters: TaskParameter,
    pub result: TaskResult,
}

pub async fn process_loop(
    mut csv_reader: AsyncReader<File>,
    mut csv_writer: AsyncWriter<File>,
    task_sender: Sender<TaskParameter>,
    result_receiver: Receiver<CompletedTask>,
) {
    let mut queue = HashSet::<String>::new();
    let mut retries = HashMap::<String, usize>::new();

    async move {
        let mut line_number: usize = 0;
        while let Some(record) = csv_reader.records().next().await {
            line_number += 1;
            let record = match record {
                Ok(r) => r,
                Err(err) => {
                    error!("line {}: skipping record ({})", line_number, err);
                    continue;
                }
            };
            let url = match record.get(0) {
                None => {
                    info!("line {}: skipping empty line", line_number);
                    continue;
                }
                Some(url) => url.to_string(),
            };
            if let Some(ts) = record.get(1) {
                if !ts.is_empty() {
                    info!("line {}: Skipping {} done at {}", line_number, url, ts);
                    if let Err(e) = csv_writer
                        .write_record(&[url.clone(), ts.to_string(), "".to_string()])
                        .await
                    {
                        error!("Error while writing back completed {}: {}", url, e);
                        break;
                    }
                    continue;
                }
            }
            queue.insert(url.clone());
            if let Err(e) = task_sender
                .send(TaskParameter {
                    line: line_number,
                    url: url.to_string(),
                })
                .await
            {
                error!("Cannot send {} for processing, stopping ({})", url, e);
                break;
            }
        }

        if queue.is_empty() {
            // this check is only done after getting a result. need this one to exit when the file
            // is empty, or it'll wait for a result that never comees.
            task_sender.close();
        }

        while let Ok(completed_task) = result_receiver.recv().await {
            let record = match completed_task.result {
                Ok(ts) => {
                    info!(
                        "Main: {} done at {:?}",
                        completed_task.parameters.url,
                        ts.to_rfc3339()
                    );
                    [
                        completed_task.parameters.url.clone(),
                        ts.to_rfc3339(),
                        "".to_string(),
                    ]
                }
                Err(err) => match err {
                    ProcessError::Unrecoverable(msg) => {
                        info!(
                            "Main: {} unrecoverable failure: {}",
                            completed_task.parameters.url, msg
                        );
                        [completed_task.parameters.url.clone(), "".to_string(), msg]
                    }
                    ProcessError::Retry(msg) => {
                        let retry = retries
                            .get(&completed_task.parameters.url)
                            .cloned()
                            .unwrap_or(0);
                        if retry > 5 {
                            info!("Main: {} failed: {}", completed_task.parameters.url, msg);
                            [completed_task.parameters.url.clone(), "".to_string(), msg]
                        } else {
                            retries.insert(completed_task.parameters.url.clone(), retry + 1);
                            info!(
                                "Main: {} failed: {}, retry {}",
                                completed_task.parameters.url, msg, retry
                            );

                            if let Err(e) = task_sender.send(completed_task.parameters.clone()).await {
                                error!(
                                    "Cannot send {} for processing {}",
                                    completed_task.parameters.url, e
                                );
                                [completed_task.parameters.url.clone(), "".to_string(), msg]
                            } else {
                                continue;
                            }
                        }
                    }
                },
            };
            queue.remove(&completed_task.parameters.url);
            if queue.is_empty() {
                task_sender.close();
            }

            if let Err(e) = csv_writer.write_record(&record).await {
                error!("Error while writing back {} ({})", record.join(";"), e)
            }
        }

        if !queue.is_empty() {
            info!("writing back {} unprocessed records", queue.len());
            for url in queue {
                if let Err(e) = csv_writer
                    .write_record(&[url.clone(), "".to_string(), "".to_string()])
                    .await
                {
                    error!("Error {} while writing {}", e, url)
                }
            }
        } else {
            info!("All records processed")
        }
    }.await
}

pub async fn prepare_workers<FutureFactory, FutureFactoryResult, FutureProcessor, FutureResult>(
    workers_count: usize,
    task_receiver: Receiver<TaskParameter>,
    result_sender: Sender<CompletedTask>,
    shutdown_receiver: Receiver<Void>,
    process_entry_factory: FutureFactory,
) where
    FutureFactory: Fn(String) -> FutureFactoryResult + Clone + Send + Sync + 'static,
    FutureFactoryResult: Future<Output=FutureProcessor> + Send,
    FutureProcessor: FnMut(TaskParameter) -> FutureResult + Send,
    FutureResult: Future<Output=TaskResult> + Send
{
    let mut workers = vec![];
    for n in 0..workers_count {
        let task_receiver = task_receiver.clone();
        let result_sender = result_sender.clone();
        let shutdown_receiver = shutdown_receiver.clone();
        let process_entry_factory = process_entry_factory.clone();

        workers.push(task::spawn(async move {
            let mut process_entry = process_entry_factory(format!("Worker {}", n)).await;

            while let Ok(task_parameters) = task_receiver.recv().await {
                let completed_task = if !shutdown_receiver.is_closed() {
                    let result = process_entry(task_parameters.clone()).await;
                    CompletedTask {
                        parameters: task_parameters.clone(),
                        result: result.clone(),
                    }
                } else {
                    CompletedTask {
                        parameters: task_parameters.clone(),
                        result: Err(ProcessError::Unrecoverable("Shutdown".to_string())),
                    }
                };

                if let Err(err) = result_sender.send(completed_task.clone()).await {
                    error!("{}: unable to send result for {}={:?}, exiting task ({})", n, task_parameters.url, completed_task.result, err);
                    break;
                }
            }

            info!("{}: graceful exit", n);
        }));
    }

    join_all(workers).await;
}

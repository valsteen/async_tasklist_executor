use std::collections::{HashMap, HashSet};

use async_std::channel::{Receiver, Sender};
use async_std::fs::File;
use async_std::task::{block_on, spawn};
use chrono::{DateTime, Local};
use csv_async::{AsyncReader, AsyncWriter};
use futures::StreamExt;
use futures::{select, FutureExt};
use log::{error, info};

pub mod process_entry;

pub enum Void {}

#[derive(Clone, Debug)]
pub enum ProcessError {
    Unrecoverable(String),
    Retry(String),
}

#[derive(Clone)]
pub struct TaskParameter {
    pub line: usize,
    pub url: String,
}

pub type TaskResult = Result<DateTime<Local>, ProcessError>;

pub struct CompletedTask {
    pub parameters: TaskParameter,
    pub result: TaskResult,
}

pub fn process_loop(
    mut csv_reader: AsyncReader<File>,
    mut csv_writer: AsyncWriter<File>,
    task_sender: Sender<TaskParameter>,
    result_receiver: Receiver<CompletedTask>,
) {
    let mut queue = HashSet::<String>::new();
    let mut retries = HashMap::<String, usize>::new();

    block_on(async move {
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
                            if let Err(e) =
                                task_sender.send(completed_task.parameters.clone()).await
                            {
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
    });
}

pub fn prepare_workers(
    workers_count: usize,
    task_receiver: Receiver<TaskParameter>,
    result_sender: Sender<CompletedTask>,
    shutdown_receiver: Receiver<Void>,
    process_entry: &'static (impl Fn(usize, TaskParameter) -> TaskResult + Send + Sync + 'static),
) {
    for n in 0..workers_count {
        let task_receiver = task_receiver.clone();
        let result_sender = result_sender.clone();
        let shutdown_receiver = shutdown_receiver.clone();

        spawn(async move {
            let mut task_receiver = task_receiver.fuse();
            let mut shutdown_receiver = shutdown_receiver.fuse();

            loop {
                select! {
                    parameters = task_receiver.next().fuse() => match parameters {
                        Some(task_parameters) => {
                            let result = process_entry(n, task_parameters.clone());

                            if let Err(err) = result_sender.send(
                                CompletedTask {
                                    parameters: task_parameters.clone(),
                                    result: result.clone()
                                }
                            ).await {
                                error!("{}: unable to send result for {}={:?}, exiting task ({})", n, task_parameters.url, result, err);
                                break
                            }
                        },
                        None => break
                    },
                    void = shutdown_receiver.next().fuse() => match void {
                        Some(void) => match void {},
                        None => break,
                    }
                }
            }

            info!("{}: graceful exit", n);
        });
    }
}

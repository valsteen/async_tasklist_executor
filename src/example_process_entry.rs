use core::result::Result::{Err, Ok};
use core::time::Duration;

use log::{error, info};

use crate::tasklist_executor::{TaskError, TaskResult, TaskRow};
use reqwest::IntoUrl;
use std::fmt::Display;

pub async fn process_entry<Data: Display>(
    worker_id: String,
    task_row: TaskRow<Data>,
) -> TaskResult<Data>
where
    Data: Clone + IntoUrl,
{
    let client = reqwest::blocking::ClientBuilder::new()
        .connect_timeout(Duration::from_millis(4000))
        .timeout(Duration::from_millis(5000))
        .build()
        .expect("Cannot build http client");

    let request = match client.get(task_row.data.clone()).build() {
        Ok(request) => request,
        Err(e) => {
            error!(
                "[{}], line {} {}: failed to build request {}",
                worker_id, task_row.line, task_row.data, e
            );
            return TaskResult::Error(task_row, TaskError::ProcessingError(e.to_string()));
        }
    };

    match client.execute(request) {
        Ok(response) => {
            if let Err(e) = response.error_for_status() {
                error!(
                    "[{}], line {} {}: status error {}",
                    worker_id, task_row.line, task_row.data, e
                );
                TaskResult::Retry(task_row, e.to_string())
            } else {
                info!(
                    "[{}], line {} {}: success",
                    worker_id, task_row.line, task_row.data
                );
                TaskResult::Success(task_row)
            }
        }
        Err(e) => {
            // note that client.execute can return a "channel closed" error message, this is its own
            // channel, not from this library
            error!(
                "[{}], line {} {}: client.execute error {}",
                worker_id, task_row.line, task_row.data, e
            );
            TaskResult::Retry(task_row, e.to_string())
        }
    }
}

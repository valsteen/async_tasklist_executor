use core::result::Result::{Err, Ok};
use core::time::Duration;

use chrono::Local;
use log::{error, info};

use crate::{ProcessError, TaskParameter, TaskResult};

pub fn process_entry(worker: usize, task_parameter: TaskParameter) -> TaskResult {
    let client = reqwest::blocking::ClientBuilder::new()
        .connect_timeout(Duration::from_millis(4000))
        .timeout(Duration::from_millis(5000))
        .build()
        .expect("Cannot build http client");

    let request = match client.get(task_parameter.url.clone()).build() {
        Ok(request) => request,
        Err(e) => {
            error!(
                "[{}], line {} {}: failed to build request {}",
                worker, task_parameter.line, task_parameter.url, e
            );
            return Err(ProcessError::Unrecoverable(e.to_string()));
        }
    };

    let response = match client.execute(request) {
        Ok(r) => r,
        Err(e) => {
            error!(
                "[{}], line {} {}: error {}",
                worker, task_parameter.line, task_parameter.url, e
            );
            return Err(ProcessError::Retry(e.to_string()));
        }
    };

    if let Err(e) = response.error_for_status() {
        error!(
            "[{}], line {} {}: error {}",
            worker, task_parameter.line, task_parameter.url, e
        );
        return Err(ProcessError::Retry(e.to_string()));
    }

    info!(
        "[{}], line {} {}: success",
        worker, task_parameter.line, task_parameter.url
    );
    Ok(Local::now())
}

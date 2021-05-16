use crate::tasklist_executor::{RecordWriter, TaskData, TaskPayload};
use async_std::fs::{File, OpenOptions};
use async_std::stream::Stream;
use async_std::sync::{Arc, Mutex};
use async_std::task;
use csv_async::AsyncWriter;
use csv_async::StringRecord;
use futures::future::BoxFuture;
use futures::StreamExt;
use log::{error, info};
use std::fmt::{Debug, Display, Formatter};

fn make_task_row(
    record: Result<StringRecord, csv_async::Error>,
    line_number: usize,
) -> Result<TaskPayload<InputData>, String> {
    let record = match record {
        Ok(record) => record,
        Err(err) => {
            error!("line {}: skipping record ({})", line_number, err);
            return Err("invalid line".to_string());
        }
    };

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

    Ok(TaskPayload {
        line: line_number,
        data: InputData(url),
        success_ts,
        error: None,
        attempt: 0,
    })
}

#[derive(Debug, Clone)]
pub struct InputData(String);

impl Display for InputData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl TaskData for InputData {}

pub fn csv_stream(filename: String) -> Result<impl Stream<Item = TaskPayload<InputData>>, String> {
    let csv_reader = {
        let input_file = task::block_on(File::open(filename.clone()))
            .map_err(|e| format!("Cannot open file {} for reading: {}", filename, e))?;
        csv_async::AsyncReaderBuilder::new()
            .has_headers(false)
            .flexible(true)
            .delimiter(b';')
            .create_reader(input_file)
    };

    let mut line_number = 0;
    Ok(csv_reader.into_records().filter_map(move |record| {
        Box::pin(async move {
            line_number += 1;
            match make_task_row(record, line_number) {
                Ok(row) => Some(row),
                Err(_) => None,
            }
        })
    }))
}

pub struct CsvWriter {
    csv_writer: Arc<Mutex<AsyncWriter<File>>>,
}

impl CsvWriter {
    pub async fn new(filename: String) -> Result<Self, String> {
        let csv_writer = {
            let output_file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(filename.clone())
                .await
                .map_err(|e| format!("Cannot open file {} for writing: {}", filename, e))?;

            csv_async::AsyncWriterBuilder::new()
                .has_headers(false)
                .delimiter(b';')
                .flexible(true)
                .create_writer(output_file)
        };

        Ok(Self {
            csv_writer: Arc::new(Mutex::new(csv_writer)),
        })
    }
}

impl RecordWriter for CsvWriter {
    type DataType = InputData;

    fn write_record(&self, task_row: TaskPayload<Self::DataType>,
    ) -> BoxFuture<'static, Result<(), String>> {
        let record = match task_row.success_ts {
            None => StringRecord::from(vec![
                task_row.data.0,
                "".to_string(),
                task_row.error.unwrap_or_default(),
            ]),
            Some(ts) => StringRecord::from(vec![task_row.data.0, ts]),
        };

        let csv_writer = self.csv_writer.clone();

        Box::pin(async move {
            let mut csv_writer = csv_writer.lock().await;
            if let Err(e) = csv_writer.write_record(&record).await {
                error!("Error while writing back {:?} ({}), quitting", record, e);
                Err(e.to_string())
            } else {
                Ok(())
            }
        })
    }
}

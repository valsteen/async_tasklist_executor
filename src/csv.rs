use crate::tasklist_executor::{RecordWriter, TaskRow};
use async_std::fs::{File, OpenOptions};
use async_std::sync::{Arc, Mutex};
use async_std::task;
use csv_async::StringRecord;
use csv_async::{AsyncWriter, StringRecordsIntoStream};
use futures::future::BoxFuture;
use log::error;


pub fn csv_stream(filename: String) -> Result<StringRecordsIntoStream<'static, File>, String> {
    let csv_reader = {
        let input_file = task::block_on(File::open(filename.clone()))
            .map_err(|e| format!("Cannot open file {} for reading: {}", filename, e))?;
        csv_async::AsyncReaderBuilder::new()
            .has_headers(false)
            .flexible(true)
            .delimiter(b';')
            .create_reader(input_file)
    };
    Ok(csv_reader.into_records())
}


pub struct CsvWriter {
    csv_writer: Mutex<AsyncWriter<File>>,
}

impl CsvWriter {
    pub fn new(filename: String) -> Result<Self, String> {
        let csv_writer = {
            let output_file = task::block_on(
                OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(filename.clone()),
            )
            .map_err(|e| format!("Cannot open file {} for writing: {}", filename, e))?;
            csv_async::AsyncWriterBuilder::new()
                .has_headers(false)
                .delimiter(b';')
                .flexible(true)
                .create_writer(output_file)
        };

        Ok(Self {
            csv_writer: Mutex::new(csv_writer),
        })
    }
}

impl RecordWriter for CsvWriter {
    type DataType = String;

    fn write_record(
        self: &Arc<Self>,
        task_row: TaskRow<Self::DataType>,
    ) -> BoxFuture<'static, Result<(), String>> {
        let self_ = self.clone();
        let record = match task_row.success_ts {
            None => StringRecord::from(vec![
                task_row.data.to_string(),
                "".to_string(),
                task_row.error.unwrap_or_default(),
            ]),
            Some(ts) => StringRecord::from(vec![task_row.data.to_string(), ts]),
        };

        Box::pin(async move {
            let mut csv_writer = self_.csv_writer.lock().await;
            if let Err(e) = csv_writer.write_record(&record).await {
                error!("Error while writing back {:?} ({}), quitting", record, e);
                Err(e.to_string())
            } else {
                Ok(())
            }
        })
    }
}
use async_std::fs::File;
use async_std::task;
use csv_async::StringRecordsIntoStream;

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

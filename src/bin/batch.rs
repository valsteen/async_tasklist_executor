use async_std::task;
use async_std::channel::unbounded;
use std::time::Duration;
use log::{info, error, LevelFilter};
use clap::{App, Arg};
use async_std::fs::{File, OpenOptions};
use std::collections::{HashSet, HashMap};
use futures::{select, FutureExt};
use futures::stream::StreamExt;
use chrono::{DateTime, Local};

enum Void {}

async fn process(worker: usize, line: usize, url: String) -> Result<DateTime<Local>, String> {
    let client = reqwest::blocking::ClientBuilder::new().connect_timeout(Duration::from_millis(4000)).timeout(Duration::from_millis(5000)).build().expect("Cannot build http client");

    let request = match client.get(url.clone()).build() {
        Ok(request) => request,
        Err(e) => {
            error!("[{}], line {} {}: failed to build request {}", worker, line, url, e);
            return Err(e.to_string());
        }
    };

    let response = match client.execute(request) {
        Ok(r) => r,
        Err(e) => {
            error!("[{}], line {} {}: error {}", worker, line, url, e);
            return Err(e.to_string());
        }
    };

    if let Err(e) = response.error_for_status() {
        error!("[{}], line {} {}: error {}", worker, line, url, e);
        return Err(e.to_string());
    }

    info!("[{}], line {} {}: success", worker, line, url);
    Ok(Local::now())
}

fn main() -> Result<(), String> {
    env_logger::builder().filter_level(LevelFilter::Info).format_timestamp_secs().init();

    let arg_matches = App::new("Vendor portal user creation")
        .arg(Arg::with_name("input").takes_value(true).long("input").required(true))
        .arg(Arg::with_name("output").takes_value(true).long("output").required(true))
        .arg(Arg::with_name("workers").takes_value(true).long("workers").required(false).default_value("4"))
        .get_matches();

    let workers: usize = arg_matches.value_of("workers").unwrap().parse().expect("Numerical value expected");

    let mut csv_reader = {
        let input_filename = arg_matches.value_of("input").unwrap();
        let input_file = task::block_on(File::open(input_filename)).map_err(|e| {
            format!("Cannot open file {} for reading: {}", input_filename, e)
        })?;
        csv_async::AsyncReaderBuilder::new().has_headers(false).flexible(true).create_reader(input_file)
    };

    let mut csv_writer = {
        let output_filename = arg_matches.value_of("output").unwrap();
        let output_file = task::block_on(OpenOptions::new().write(true).create_new(true).open(output_filename)).map_err(|e| {
            format!("Cannot open file {} for writing: {}", output_filename, e)
        })?;
        csv_async::AsyncWriterBuilder::new().has_headers(false).create_writer(output_file)
    };

    let (task_sender, task_receiver) = unbounded::<(usize, String)>();
    let (result_sender, result_receiver) = unbounded::<((usize, String), Result<DateTime<Local>, String>)>();
    let (shutdown_sender, shutdown_receiver) = unbounded::<Void>();

    for n in 0..workers {
        let task_receiver = task_receiver.clone();
        let result_sender = result_sender.clone();
        let shutdown_receiver = shutdown_receiver.clone();

        task::spawn(async move {
            let mut task_receiver = task_receiver.fuse();
            let mut shutdown_receiver = shutdown_receiver.fuse();

            loop {
                select! {
                    parameters = task_receiver.next().fuse() => match parameters {
                        Some((line, url)) => {
                            let result = process(n, line, url.clone()).await;

                            if let Err(err) = result_sender.send(((line, url.clone()), result.clone())).await {
                                error!("{}: unable to send result for {}={:?}, exiting task ({})", n, url, result, err);
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
    drop(task_receiver);
    drop(result_sender);

    let mut queue = HashSet::<String>::new();
    let mut retries = HashMap::<String, usize>::new();

    task::block_on(async move {
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
                Some(url) => url.to_string()
            };
            if let Some(ts) = record.get(1) {
                if !ts.is_empty() {
                    info!("line {}: Skipping {} done at {}", line_number, url, ts);
                    if let Err(e) = csv_writer.write_record(&[url.clone(), ts.to_string(), "".to_string()]).await {
                        error!("Error while writing back completed {}: {}", url, e);
                        break;
                    }
                    continue;
                }
            }
            queue.insert(url.clone());
            if let Err(e) = task_sender.send((line_number, url.to_string())).await {
                error!("Cannot send {} for processing, stopping ({})", url, e);
                break;
            }
        }

        ctrlc::set_handler(move || {
            shutdown_sender.close();
        }).expect("Error setting Ctrl-C handler");

        while let Ok(((line_number, url), result)) = result_receiver.recv().await {
            let record = match result {
                Ok(ts) => {
                    info!("Main: {} done at {:?}", url, ts.to_rfc3339());
                    [url.clone(), ts.to_rfc3339(), "".to_string()]
                }
                Err(err) => {
                    let retry = retries.get(&url).cloned().unwrap_or(0);
                    if retry > 5 {
                        info!("Main: {} failed: {}", url, err);
                        [url.clone(), "".to_string(), err]
                    } else {
                        retries.insert(url.clone(), retry+1);
                        info!("Main: {} failed: {}, retry {}", url, err, retry);
                        if let Err(e) = task_sender.send((line_number, url.to_string())).await {
                            error!("Cannot send {} for processing {}", url, e);
                            [url.clone(), "".to_string(), err]
                        } else {
                            continue;
                        }
                    }
                }
            };
            queue.remove(&url);
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
                if let Err(e) = csv_writer.write_record(&[url.clone(), "".to_string(), "".to_string()]).await {
                    error!("Error {} while writing {}", e, url)
                }
            }
        } else {
            info!("All records processed")
        }
    });

    Ok(())
}

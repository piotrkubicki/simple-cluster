#[macro_use] extern crate log;

use std::env;
use std::error::Error;
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::task;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};

use futures::future::join_all;

use reqwest::Client;
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{RetryTransientMiddleware, policies::ExponentialBackoff};

use serde::Deserialize;

use aws_sdk_s3 as s3;

use http_request::{Route, Router};

const IP_ADDRES: &str = "0.0.0.0";
const PORT_NUMBER: isize = 5100;

#[derive(Debug, Deserialize, PartialEq)]
enum WorkerStatus {
    IDLE,
    RUNNING,
    ERROR,
}

struct Worker {
    url: String,
    status: WorkerStatus,
}

impl Worker {
    fn new(url: String) -> Worker {
        let status = WorkerStatus::IDLE;
        Worker {
            url,
            status,
        }
    }

    async fn healthcheck(&mut self, client: &ClientWithMiddleware) {
        let healthcheck = client.get(format!("{}/healthcheck", self.url)).send();
        match tokio::time::timeout(tokio::time::Duration::from_secs(2), healthcheck).await {
            Ok(res) => {
                match res {
                    Ok(res) => {
                        if !res.status().is_success() {
                            self.status = WorkerStatus::ERROR;
                        } else {
                            match res.json().await {
                                Ok(status) => self.status = status,
                                Err(e) => error!("{e}"),
                            }
                        }
                    },
                    Err(_) => self.status = WorkerStatus::ERROR,
                }
            },
            Err(e) => error!("Something goes wrong {e}"),
        }
    }
}

fn workers_healthcheck(workers: Arc<Mutex<Vec<Worker>>>, client: ClientWithMiddleware, wait_time_in_sec: u64) {
    let workers = workers.clone();
    let client = client.clone();
    task::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(wait_time_in_sec));
        loop {
            interval.tick().await;
            debug!("Checkings workers health...");
            let mut workers = workers.lock().await;
            let tasks: Vec<_> = workers.iter_mut().map(|worker| worker.healthcheck(&client)).collect();
            join_all(tasks).await;

            let mut failed_workers: Vec<usize> = Vec::new();
            for (i, worker) in workers.iter().enumerate() {
                if worker.status == WorkerStatus::ERROR {
                    failed_workers.push(i);
                }
            }
            for failed_worker in failed_workers {
                workers.remove(failed_worker);
            }
            debug!("Health check done");
        }
    });
}

async fn get_idle_worker_index(workers: &Mutex<Vec<Worker>>) -> usize {
    loop {
        {
            let workers = workers.lock().await;
            for (i, worker) in workers.iter().enumerate() {
                if worker.status == WorkerStatus::IDLE {
                    return i;
                }
            }
        }
        sleep(Duration::from_secs(3)).await;
    }
}

fn run_task(workers: Arc<Mutex<Vec<Worker>>>, client: ClientWithMiddleware) -> Route {
    Box::new(move |req, res| {
        let workers = workers.clone();
        let client = client.clone();

        match &req.body {
            Some(task) => {
                let bucket = task.clone();
                task::spawn(async move {
                    let config = aws_config::load_from_env().await;
                    let s3_client = s3::Client::new(&config);
                    let res = s3_client.list_objects_v2().bucket(&bucket).send().await;

                    match res {
                        Ok(res) => {
                            for obj in res.contents().unwrap_or_default() {
                                let file = obj.key().unwrap_or_default().to_string();
                                let path = format!("{bucket}/{file}");
                                let worker_index = get_idle_worker_index(workers.as_ref()).await;
                                let mut workers = workers.as_ref().lock().await;
                                let _ = client.post(format!("{}/task", workers[worker_index].url))
                                    .header("content-length", path.len())
                                    .body(path).send().await;
                                workers[worker_index].status = WorkerStatus::RUNNING;
                            }
                        },
                        Err(e) => error!("Something goes wrong! {}", e),
                    }
                });
                task::spawn(async move {
                    res.status("OK".to_string())
                        .status_code(200)
                        .body("OK".to_string())
                        .send().await;
                });
            },
            None => warn!("No data received for processing"),
        };

        Ok(())
    })
}

fn process_result() -> Route {
    Box::new(|req, res| {
        task::spawn(async move{
            let data = req.body.unwrap();
            info!("Result: {:?}", data);
            res.status("OK".to_string())
                .status_code(200)
                .body("OK".to_string())
                .send().await;
        });

        Ok(())
    })
}

fn register_worker(workers: Arc<Mutex<Vec<Worker>>>) -> Route {
    Box::new(move |req, res| {
        let workers = workers.clone();
        task::spawn(async move {
            let worker_url = req.body.as_ref().unwrap();
            let worker = Worker::new(worker_url.to_string());
            workers.lock().await.push(worker);
            res.status("OK".to_string())
                .status_code(200)
                .body("OK".to_string())
                .send().await;
        });

        Ok(())
    })
}

fn check_workers_status(workers: Arc<Mutex<Vec<Worker>>>) -> Route {
    Box::new(move |_req, res| {
        let workers = workers.clone();
        task::spawn(async move {
            let workers = workers.lock().await;
            let mut response = String::new();
            for worker in workers.iter() {
                response.push_str(&format!("URL: {} STATUS: {:?}\n", worker.url, worker.status));
            }
            res.body(response)
                .send().await;
        });

        Ok(())
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();

    let wait_time_in_sec: u64 = env::var("WAIT_TIME").unwrap_or("1".to_string()).parse().unwrap();
    let workers: Arc<Mutex<Vec<Worker>>> = Arc::new(Mutex::new(Vec::new()));
    let retry_policy = ExponentialBackoff::builder().build_with_max_retries(10);
    let client = ClientBuilder::new(Client::new())
        .with(RetryTransientMiddleware::new_with_policy(retry_policy))
        .build();

    let mut router = Router::new();
    router.register("/register".to_string(), register_worker(workers.clone()));
    router.register("/status".to_string(), check_workers_status(workers.clone()));
    router.register("/task".to_string(), run_task(workers.clone(), client.clone()));
    router.register("/result".to_string(), process_result());

    let addr = format!("{}:{}", IP_ADDRES, PORT_NUMBER);
    let listener = TcpListener::bind(&addr).await?;

    workers_healthcheck(workers.clone(), client.clone(), wait_time_in_sec);

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                router.process(stream).await;
            },
            Err(e) => {
                error!("{e}");
                break;
            }
        }
    }

    Ok(())
}

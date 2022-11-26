use std::str;
#[macro_use] extern crate log;

use std::io::BufRead;
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::error::Error;
use std::collections::HashMap;

#[derive(Debug)]
pub enum Method {
    GET,
    POST
}

#[derive(Debug)]
pub struct HttpRequest {
    pub method: Option<Method>,
    pub uri: Option<String>,
    pub version: Option<String>,
    pub headers: Option<HashMap<String, String>>,
    pub body: Option<String>,
}

impl HttpRequest {
    fn new() -> Self {
        HttpRequest {
            method: None,
            uri: None,
            version: None,
            headers: None,
            body: None,
        }
    }

    fn set_head(&mut self, method: Method, uri: String, version: String, headers: HashMap<String, String>) {
        self.method = Some(method);
        self.uri = Some(uri);
        self.version = Some(version);
        self.headers = Some(headers);
    }

    fn set_body(&mut self, body: String) {
        self.body = Some(body);
    }

    fn parse_head(head: &Vec<&str>) -> (Method, String, String) {
        let [method, uri, version] = match head[0].split(" ").collect::<Vec<&str>>() {
            head if head.len() >= 3 => {
                [head[0], head[1], head[2]]
            }
            _ => panic!("Malformed header"),
        };
        info!("Method name: {}", method);
        let method = match method {
            "GET" => Method::GET,
            "POST" => Method::POST,
            _ => panic!("Unrecognised method name!"),
        };

        (method, uri.to_string(), version.to_string())
    }

    fn parse_headers(head: &Vec<&str>) -> HashMap<String, String> {
        let mut headers = HashMap::new();

        for header in &head[1..] {
            let (key, val) = header.rsplit_once(":").unwrap();
            headers.insert(key.trim().to_lowercase(), val.trim().to_lowercase());
        }

        headers
    }

    async fn pull(stream: &mut TcpStream) -> Vec<u8> {
        let mut res: Vec<u8> = Vec::new();
        let mut buf = [0; 1024];
        match stream.read(&mut buf).await {
            Ok(n) => {
                let buf = &buf[..n];
                res.extend_from_slice(buf);
            },
            Err(e) => {
                eprintln!("Cannot read from stream: {e}");
            }
        }

        res
    }

    pub async fn decode(mut stream: TcpStream) -> Result<HttpRequest, Box<dyn Error>> {
        let mut buf: Vec<u8> = Vec::new();
        let data = HttpRequest::pull(&mut stream).await;
        buf.extend_from_slice(&data);
        let mut body_buf: String = String::new();
        let mut http_request = HttpRequest::new();

        loop {
            if  body_buf.len() == 0 {
                let data: Vec<_> = str::from_utf8(&buf).unwrap().rsplit("\r\n\r\n").collect();
                let data: Option<(&str, &str)> = match data.len() {
                    n if n > 2 => panic!("Malformed request!"),
                    2 => Some((data[1], data[0])),
                    n if n < 2 => {
                        let data = HttpRequest::pull(&mut stream).await;
                        buf.extend_from_slice(&data);
                        None
                    },
                    _ => panic!("Unknown"),
                };

                match data {
                    Some((head, body)) => {
                        let head: Vec<&str> = head.lines().collect();
                        body_buf.push_str(&body);
                        let (method, uri, version) = HttpRequest::parse_head(&head);
                        let headers = HttpRequest::parse_headers(&head);
                        http_request.set_head(method, uri, version, headers);
                    },
                    None => continue,
                }
            } else {
                match http_request.headers.as_ref().unwrap().get("content-length") {
                    Some(content_len) => {
                        let content_len = content_len.parse::<usize>();
                        if content_len == Ok(body_buf.len()) {
                            http_request.set_body(body_buf);
                            break;
                        } else {
                            let data = HttpRequest::pull(&mut stream).await;
                            let data = str::from_utf8(&data).unwrap();
                            body_buf.push_str(&data);
                        }
                    },
                    None => {
                        break;
                    }
                }
            }
        }

        Ok(http_request)
    }
}

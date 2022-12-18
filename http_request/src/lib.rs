#[macro_use] extern crate log;

use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::error::Error;
use std::collections::HashMap;
use std::str;

#[derive(Debug, PartialEq)]
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

    async fn pull(stream: &mut OwnedReadHalf) -> Vec<u8> {
        let mut res: Vec<u8> = Vec::new();
        let mut buf = [0; 1024];
        match stream.read(&mut buf).await {
            Ok(n) => {
                let buf = &buf[..n];
                res.extend_from_slice(buf);
            },
            Err(e) => eprintln!("Cannot read from stream: {e}"),
        }

        res
    }

    pub async fn decode(mut stream: OwnedReadHalf) -> Result<HttpRequest, Box<dyn Error>> {
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
                        if http_request.method.as_ref().unwrap() == &Method::GET {
                            break;
                        }
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

pub struct HttpResponse {
    pub version: String,
    pub status_code: usize,
    pub status: String,
    pub body: String,
    pub headers: HashMap<String, String>,
    write_stream: OwnedWriteHalf,
}

impl HttpResponse {

    pub fn new(write_stream: OwnedWriteHalf) -> Self {
        let headers: HashMap<String, String> = HashMap::new();
        HttpResponse {
            version: "HTTP/1.1".to_string(),
            status_code: 200,
            status: "OK".to_string(),
            body: "".to_string(),
            headers,
            write_stream,
        }
    }

    pub fn add_header(mut self, header: String, value: String) -> Self {
        self.headers.insert(header, value);
        self
    }

    pub fn status(mut self, status: String) -> Self {
        self.status = status;
        self
    }

    pub fn status_code(mut self, status_code: usize) -> Self {
        self.status_code = status_code;
        self
    }

    pub fn body(mut self, body: String) -> Self {
        self.body = body;
        self
    }

    pub async fn send(&mut self) {
        let head = format!("{} {} {}", self.version, self.status_code, self.status);
        let mut headers = self.headers.clone();
        headers.insert("Content-Length".to_string(), (self.body.len()).to_string());
        let headers = headers.iter().map(|(key, val)| format!("{}: {}", key, val)).collect::<Vec<String>>().join("\n");
        let response = format!("{}\r\n{}\r\n\r\n{}", head, headers, self.body);
        let _ = self.write_stream.write_all(&response.as_bytes()).await;
    }
}

pub type Route = Box<dyn Fn(HttpRequest, HttpResponse) -> Result<(), Box<dyn Error>>>;

pub struct Router {
    routes: HashMap<String, Route>,
}

impl Router {
    pub fn new() -> Self {
        Router {
            routes: HashMap::new(),
        }
    }

    pub fn register(&mut self, url: String, action: Route) {
        self.routes.insert(url, action);
    }

    pub async fn process(&self, stream: tokio::net::TcpStream) {
        let (read_stream, write_stream) = stream.into_split();
        let req = HttpRequest::decode(read_stream).await;
        let res = HttpResponse::new(write_stream);
        let uri = req.as_ref().unwrap().uri.as_ref();
        let route = self.routes.get(uri.unwrap());
        let _ = match route {
            Some(route) => (route)(req.unwrap(), res),
            None => {
                res.status("NOT_FOUND".to_string())
                    .status_code(404)
                    .body("PAGE NOT FOUND".to_string())
                    .send().await;
                Ok(())
            },
        };
    }
}

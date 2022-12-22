FROM rust:1.64-buster as builder

WORKDIR app

COPY ./commons ./commons/

COPY ./master-node ./src/ 

RUN cargo install --path ./src/

FROM debian:buster-slim

RUN apt update && apt -y install libssl1.1 ca-certificates && update-ca-certificates

COPY --from=builder /usr/local/cargo/bin/master-node /usr/local/bin/master-node

CMD ["master-node"]
# time-series-benchmark-playground

## Prerequisite

### Setup Greptime instance

Binary or docker

```bash
./greptime standalone start
```

### Install usql and set-up usql

```bash
brew install usql 
```

```bash
usql mysql://127.0.0.1:4002/
```

## Generate data

```bash
cargo run generate_data | gzip > ./data.gz
```

## Generate insert statements

```bash
cargo run load
```

## Generate query statements
todo

# Rclone MTProto Backend

This is an out-of-tree backend for rclone that provides access  via the **MTProto** protocol. It uses the [gogram](https://github.com/amarnathcjd/gogram) library to communicate with MTProto's API, allowing you to use MTProto forums (supergroups with topics) as a remote file system (S3-compatible).

## Features

- Configurable via rclone's standard configuration system
- List, read, and write files in MTProto supergroup forums
- Change notification polling for forum topic updates
- Server-side directory (topic) moves

## Building

Clone this repository and build with `go build` to produce an rclone binary with the `mtproto` backend included:

```shell
git clone https://github.com/Niruchie/rclone_backend_mtproto
cd rclone_backend_mtproto
go build
```

## Configuration

The `mtproto` backend uses MTProto API. You will need valid MTProto API credentials to authenticate.

Configure a new remote with:

```shell
rclone config
```

And follow the prompts to set up your remote.


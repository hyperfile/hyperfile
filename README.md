![Hyperfile - Random Read Write Object Format](./docs/assets/images/hyperfile-header.svg)

# Hyperfile - aka Random Read Write Object Format

A file-like interface abstraction layer provides random read and write capabilities for S3 (object storage).

## Overview

S3 (object storage) provides high availability and high durability, on-demand billing, low-cost, and elastic storage space for various data.

Immutability comes as S3's nature(despite S3 Express One Zone now support [append](https://aws.amazon.com/about-aws/whats-new/2024/11/amazon-s3-express-one-zone-append-data-object/)), which is a key distinguishes between object storage and file storage, for most scenarios, immutability is acceptable.

However, in scenarios such as log collection, disk image file, backup, etc., the underlying storage system needs to provide append writes, random writes, sparse capability and other characteristics.

Hyperfile bridges the gap between file storage and object storage by providing a POSIX-like file API for access data direct on S3.

It enables applications to perform random read, write and truncate in aligned with block-boundary by implements a abstraction layer to reconstructing data index with Random Read Write Object Format.

With Hyperfile, application can perform any form of random read write directly on S3.

Hyperfile runs entirely in user space.

## Terminology

* **Staging** - where data is stored in random read write format.
* **Meta block** - index of file address space organized in B+tree.
* **Data block** - block chunks of file content.
* **Segment** - container of all modified(dirty) data, including header, meta blocks and data blocks.

## Concept

Hyperfile organized persistent data on S3 in random read write (log-structured) format by reconstructing data index using B+tree structure.

When flush happens, all modified(dirty) data blocks together with it's metadata blocks are collected and constructed into on-disk segment format.

![Hyperfile B+tree Core](./docs/assets/images/hyperfile-light.svg#gh-light-mode-only)![Hyperfile B+tree Core](./docs/assets/images/hyperfile-dark.svg#gh-dark-mode-only)

### Copy-on-Write (CoW)

Hyperfile never overwrite persisted data on S3, every new written segment will be assigned with a global sequentially increasing id.

### Log-Structured Characteristics

Persistent data organized in Log-Structured style, which means, Hyperfile has the native capability to implement continous snapshot, time travel and point-in-time recovery for better data protection.

## Features

* B+tree data structure based on [btree-ondisk](https://github.com/daiyy/btree-ondisk).
* Read/write data direct on S3 with file-like API.
* Support S3 and S3 Express One Zone as staging area.
* Optional: WAL(Write-Ahead-Log) support to perserve durability of unflushed data.
* Optional: data blocks cache and meta blocks cache on local disk to accelerate read path.

## Consistency

Hyperfile guaranteed to be consistent after each successful flush.

## Limitations

* Read/write path IO amplify.
* Extra storage space for metadata blocks.
* On-disk data layout changed (in contrast of a real S3 object).
* Segments prune/compaction effort.

## Examples

See [examples](examples/) for how to use.

## Under Developement

:warning: This library is currently under developement and is **NOT** recommended for production.

## Credits

In loving memory of my father, Mr. Dai Wenhua, Who bought me my first computer.

## License

This project is licensed under the Apache-2.0 License.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

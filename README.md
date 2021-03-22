# Notes

This LSM-trie implementation does not use any user-space cache. Its read performance is bottlenecked by I/O.
If you're looking for a high-performance SSD KV-store for fast write, read, and range search, take a look at [RemixDB](https://github.com/wuxb45/remixdb).

# Build

Compiler:

  clang or gcc (change it in Makefile).

  openssl for SHA1 function.

Build:

    $ make all

# Run
Get help information of the get/set tests:

    $ ./mixed_test -h

Get the help information to run the read performance with different store sizes.

    $ ./staged_read -h

# storage space configuration

You need to change the cm\_conf1.txt to configure the storage devices (or the file specified with '-c' option).

At the beginning of the file, every two lines describe a storage device.
It could be a raw block device (e.g., /dev/sdb), or a regular file in your file system.
If it's a block device, than the second line would be set to '0', which means the capacity of the device should be automatically detected.
Make sure you have permission to use raw block devices (being the root user or being added to the disk group).

    /dev/sdb  -- block device
    0         -- auto-detect capacity

Otherwise, for regular files you should specify the file size, in the unit of GBs.

    /home/me/bigfile -- a regular file
    100           -- allocate 100GB for this file

In this way, you can add a mix of multiple devices/files.
After declaring all the storage options, add a '$' at the next line to mark the end of the storage section.

After the '$' line, you need to give the mapping information from level ID to storage ID, both starting from 0.
The first line specifies the device ID used by bloom-container.
The rest five lines specify the mapping from Level 0 to Level 4. For example:

    /dev/sdb  -- Storage 0
    0
    /dev/sdc  -- Storage 1
    0
    $
    0         -- Bloom Containers use sdb
    0         -- Level 0 is on sdb
    0         -- Level 1 is on sdb
    1         -- Level 2 is on sdc
    1         -- Level 3 is on sdc
    1         -- Level 4 is on sdc

A simplest configuration would be putting everything on one device. It looks like this:

    /dev/sdb
    0
    $
    0
    0
    0
    0
    0
    0

Or similarly using a file:

    mybigfile
    100
    $
    0
    0
    0
    0
    0
    0


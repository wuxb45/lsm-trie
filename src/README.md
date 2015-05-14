# Build

You need:

> clang or gcc (set it in Makefile).

> openssl for SHA1 function.

To build:

> $ make all

# Run
Get the help information to run the get/set tests:

> $ ./mixed_test -h

Get the help information to run the read performance with different store sizes.

> $ ./staged_read -h

# storage space configuration

You may need to change the cm_conf1.txt to set the correct storage devices (or the file indicated by '-c' option).

It is a naive and ugly configuration format.
Start from the beginning, each two lines indicate a storage device.
It could be either a raw block device (e.g., /dev/sdb), or a regular file in your file system.
If it's a block device, than the second line would be set to '0'. It means the capacity of the device would be automatically detected.

    /dev/sdb  -- block device
    0         -- auto-detect capacity

Otherwise, you may give the number of GBs you want for the file.

    /root/bigfile -- a regular file
    100           -- allocate 100GB for this file

In this way, add as many as devices/files as you want. After this, a '$' at the new line marks the end of this section.

After the '$' line, you need to give the mapping information from which level to which storage. number is used to reference the storage starting from '0'. The first line indicates the device used by bloom-container. The rest five lines indicates the mapping from level-0 to level-4. For example:

    /dev/sdb
    0
    /dev/sdc
    0
    $
    0
    0
    0
    1
    1
    1

In this example, we use two block devices, sdb and sdc. sdb is assigned to bloom-container/level-0/level-1. sdc is assigned to level-2 to level-4.

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

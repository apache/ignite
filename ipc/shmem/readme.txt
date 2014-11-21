GridGain Shared memory IPC library
-------------------

GridGain shared memory IPC library implement exchange via shared memory for GridGain.

Building on Linux and Mac OS X
-------------------

For builing on Linux and Mac OS X the shared memory library uses the GNU Autotools toolchain.
Go to the 'ipc/shmem/' directory and run:

    ./configure

You can also do

    ./configure --help

to see all available command line options. Once the configure script finishes, you can run a make:

    make install

This will build and install the shared memory library and the headers into the default location on your
system (which is usually '/usr/local').

Usage with GridGain
-------------------

Copy compiled library to folder that already listed in 'java.library.path'
with name in form: 'libggshmem-<gridgain-version>.<extention>'.
Note: Grid should be restarted.

Apache Ignite Shared memory IPC library
---------------------------------------

Apache Ignite shared memory IPC library implement exchange via shared memory for Apache Ignite.

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

Usage with Apache Ignite
-------------------

Copy compiled library to folder that already listed in 'java.library.path'
with name in form: 'libigniteshmem-<ignite-version>.<extension>'.
Note: Ignite should be restarted.

**************************************************************************************
*** Additional notes:
*** - 'make install' should be run under sudo
*** - if you have problems with <jni.h>, remember that JAVA_HOME have to be set not only for current user, but and for sudo user (check it by typing 'sudo -E env | grep JAVA_HOME')
*** - if you do all right, but it still no work you can just replace manually $(JAVA_HOME) on your java home path at Makefiles.* . :-)
**************************************************************************************

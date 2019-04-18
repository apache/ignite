GridGain C++
==================================

GridGain C++ provides data grid functionality.
Using GridGain C++ APIs you can perform concurrent operations on
the data stored in cache.

GridGain C++ can access cluster and share data with .Net and
Java applications using binary object format.

Support for the following will be added in next releases:
 * ACID transactions management.
 * Distributed locks.
 * Asynchronous operations.
 * Cache SQL continuous queries.
 * Event listening.
 * Compute grid functionality.

Full source code is provided. Users should build the library for intended platform.

For build instructions please refer to DEVNOTES.txt.

For details on ODBC driver installation and usage please refer to
$IGNITE_HOME/platforms/cpp/odbc/README.txt.

Linux info
==============

Files list:

 * ignite - executable to start standalone GridGain C++ node.
 * libignite.so - GridGain C++ API library.
 * libignite-odbc.so - GridGain ODBC driver.
 * libignite-thin-client.so - GridGain C++ thin client library.
 
Development:

 * IGNITE_HOME environment variable must be set to GridGain installation directory.
 * Once both libraries are built and installed, required headers are placed in the
   "/usr/local/include/ignite" directory.
 * GridGain C++ depends on jni.h file located inside ${JAVA_HOME}/include directory.
   Add this directory to headers search path: "-I${JAVA_HOME}/include".
 * Library is placed in the "/usr/local/lib" directory. Link it to your project: "-lignite".
 * GridGain depends on "libjvm.so" library shipped with Java. Typically this library is
   located inside $JAVA_HOME/jre/lib/amd64/server directory. Ensure that LD_LIBRARY_PATH
   environment variable points to this directory.
 * To start GridGain as a standalone node use "ignite" binary.

 
Windows info
===============

Files list:

 * ignite.exe - executable to start standalone GridGain C++ node.
 * ignite.core.dll - GridGain C++ API library.
 * ignite.odbc.dll - GridGain ODBC driver.
 * ignite.thin-client.dll - GridGain thin C++ client.
 
Development:

 * IGNITE_HOME environment variable must be set to GridGain installation directory.
 * Update Include Directories in Project Properties with paths to:
   * $(IGNITE_HOME)\platforms\cpp\common\include
   * $(IGNITE_HOME)\platforms\cpp\common\os\win\include
   * $(IGNITE_HOME)\platforms\cpp\jni\include
   * $(IGNITE_HOME)\platforms\cpp\jni\os\win\include
   * $(IGNITE_HOME)\platforms\cpp\binary\include
   * $(IGNITE_HOME)\platforms\cpp\core\include to use thick client
   * $(IGNITE_HOME)\platforms\cpp\thin-client\include to use thin client
   * $(JAVA_HOME)\include
   * $(JAVA_HOME)\include\win32
 * Update Library Directories with path to the built binaries
 * Update Linker\Input\Additional Dependencies in Project Properties with path to
   * ignite.common.lib
   * ignite.binary.lib
   * ignite.jni.lib to use thick client
   * ignite.core.lib to use thick client
   * ignite.network.lib to use thin client
   * ignite.thin-client.lib to use thin client
 * Make sure that your application is aware about ignite.jni.dll and ignite.core.dll or
   ignite.thin-client.dll libraries. The easiest way to achieve this is to either make
   sure these files are in %PATH%, or to put them into the output directory of your
   project with help of PostBuild events.
 * To start GridGain as a standalone node or Windows service use ignite.exe

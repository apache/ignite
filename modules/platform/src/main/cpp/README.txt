Ignite for C++
==================================

Ignite C++ provides data grid functionality.
Using Ignite C++ APIs you can execute perform concurrent operations on
the data stored in cache.

Ignite C++ can access cluster and share data with .Net and
Java applications using portable object format.

Support for the following will be added in next releases:
 * ACID transactions management.
 * Distributed locks.
 * Async operations.
 * Cache SQL queries and continuous queries.
 * Event listening.
 * Compute grid functionality.

Building the Library
==================================

Full source code is provided. Users should build the library for intended platform.

Building on Linux With Autotools
----------------------------------

Common Requirements:

 * GCC, g++, autotools, automake, and libtool must be installed
 * Java Development Kit (JDK) must be installed: https://java.com/en/download/index.jsp
 * JAVA_HOME environment variable must be set pointing to Java installation directory.

Building the library:

 * Build Ignite C++ helper "common" library:
     * Navigate to the directory $IGNITE_HOME/modules/platform/src/main/cpp/common
     * Execute the following commands one by one:
         * libtoolize
         * aclocal
         * autoheader
         * automake --add-missing
         * autoreconf
         * ./configure
         * make
         * make install
 * Build Ignite C++ library:
     * Navigate to the directory $IGNITE_HOME/modules/platform/src/main/cpp/core
     * Execute the following commands one by one:
         * libtoolize
         * aclocal
         * autoheader
         * automake --add-missing
         * autoreconf
         * ./configure
         * make
         * make install

NOTE: "make install" command may require superuser privileges. In this case it must be
executed as "sudo make install".

Development:

 * IGNITE_HOME environment variable must be set to Ignite installation directory.
 * Once both libraries are built and installed, required headers are placed in the
   "/usr/local/include/ignite" directory.
 * Ignite C++ depends on jni.h file located inside ${JAVA_HOME}/include directory.
   Add this directory to headers search path: "-I${JAVA_HOME}/include".
 * Library is placed in the "/usr/local/lib" directory. Link it to your project: "-lignite".
 * Ignite depends on "libjvm.so" library shipped with Java. Typically this library is located inside
   $JAVA_HOME/jre/lib/amd64/server directory. Ensure that LD_LIBRARY_PATH environment variable points to this directory.


Building on Windows with Visual Studio (tm)
----------------------------------

Common Requirements:

 * Microsoft Visual Studio (tm) 2010
 * Windows SDK 7.1
 * Java Development Kit (JDK) must be installed: https://java.com/en/download/index.jsp
 * JAVA_HOME environment variable must be set pointing to Java installation directory.

Building the library:

 * Open and build %IGNITE_HOME%\modules\platform\src\main\cpp\project\vs\ignite.sln (or ignite_86.sln if you are running 32-bit platform).

Development:

 * IGNITE_HOME environment variable must be set to Ignite installation directory.
 * Update Include Directories in Project Properties with paths to:
   * platforms\cpp\core\include
   * platforms\cpp\core\os\win\include
   * platforms\cpp\common\include
   * platforms\cpp\common\os\win\include
   * $(JAVA_HOME)\include
   * $(JAVA_HOME)\include\win32
 * Update Library Directories with path to the built binaries
 * Update Linker\Input\Additional Dependencies in Project Properties with path to
   * ignite.common.lib
   * ignite.core.lib
 * Make sure that your application is aware about ignite.common.dll and ignite.core.dll libraries. The easiest way
   to achieve this is to either make sure these files are in %PATH%, or to put them into the output directory of
   your project with help of PostBuild events.

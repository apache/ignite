Ignite C++ Examples
==================================

Common requirements
----------------------------------
 * Java Development Kit (JDK) must be installed: https://java.com/en/download/index.jsp
 * JAVA_HOME environment variable must be set pointing to Java installation directory.
 * IGNITE_HOME environment variable must be set to Ignite installation directory.
 * Ignite must be build and packaged using Maven. You can use the followin Maven command: mvn clean package -DskipTests
 * Ignite C++ must be built according to instructions for your platform.

Running examples on Linux
----------------------------------

Prerequisites:
 * GCC, g++, autotools, automake, and libtool must be installed.

To build examples execute the following commands one by one from $IGNITE_HOME/examples/platform/cpp directory:
 * libtoolize
 * aclocal
 * autoheader
 * automake --add-missing
 * autoreconf
 * ./configure
 * make

As a result several executables will appear in example's directory.

Before running examples ensure that:
 * LD_LIBRARY_PATH environment variable is set and pointing to a directory with "libjvm.so" library. Typically this
   library is located in $JAVA_HOME/jre/lib/amd64/server directory.


Running examples on Windows
----------------------------------

Prerequisites:
 * Microsoft Visual Studio (tm) 2010 or higher must be installed.
 * Windows SDK 7.1 must be installed.

Open Visual Studio solution %IGNITE_HOME%\examples\platform\cpp\project\vs\ignite-examples.sln and select proper
platform (x64 or x86). Run the solution.
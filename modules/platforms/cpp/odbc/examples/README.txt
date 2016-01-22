Apache Ignite C++ ODBC Examples
==================================

Common requirements
----------------------------------
 * ODBC Driver Manager must be present and installed on your platform.
 * Apache Ignite ODBC driver must be built and installed according to instructions for your platform.

Running examples on Linux
----------------------------------

Prerequisites:
 * GCC, g++, autotools, automake, and libtool must be installed.

To build examples execute the following commands one by one from $IGNITE_HOME/platforms/cpp/examples directory:
 * libtoolize
 * aclocal
 * autoheader
 * automake --add-missing
 * autoreconf
 * ./configure
 * make

As a result several executables will appear in example's directory.

Running examples on Windows
----------------------------------

Prerequisites:
 * Microsoft Visual Studio (tm) 2010 or higher must be installed.
 * Windows SDK 7.1 must be installed.

Open Visual Studio solution %IGNITE_HOME%\platforms\cpp\odbc\examples\project\vs\ignite-odbc-examples.sln and select proper
platform (x64 or x86). Run the solution.
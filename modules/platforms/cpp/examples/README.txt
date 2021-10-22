Apache Ignite C++ Examples
==================================

Common requirements
----------------------------------
 * Java Development Kit (JDK) must be installed: https://java.com/en/download/index.jsp
 * JAVA_HOME environment variable must be set pointing to Java installation directory.
 * IGNITE_HOME environment variable must be set to Ignite installation directory.
 * Ignite must be built and packaged using Maven. You can use the following Maven command: mvn clean package -DskipTests
 * Apache Ignite C++ must be built according to instructions for your platform. Refer to
   $IGNITE_HOME/platforms/cpp/DEVNOTES.txt for instructions.
 * For odbc-example additionally ODBC Driver Manager must be present and installed on your platform and
   Apache Ignite ODBC driver must be built and installed according to instructions for your platform. Refer to
   $IGNITE_HOME/platforms/cpp/DEVNOTES.txt for build instructions and to $IGNITE_HOME/platforms/cpp/odbc/README.txt.
   for installation instructions.

Running examples.
----------------------------------

Prerequisites:
 * C++ compiler and SDK:
  Linux and Mac OS X:
   * clang 3.9 or later
   * gcc 3.6 or later
  Windows:
   * Visual Studio 2010 or later
   * Windows SDK 7.1 or later
 * CMake >= 3.6 must be installed
 * Apache Ignite C++ should be installed. Refer to $IGNITE_HOME/platforms/cpp/DEVNOTES.txt for instructions.

To build examples execute the following commands one by one from examples root directory:
 * mkdir cmake-build-[debug|release]
 * cd ./cmake-build-[debug|release]
 * cmake .. -DCMAKE_BUILD_TYPE=[Release|Debug] [-DIGNITE_CPP_DIR=<ignite_install_dir>]
 * cmake --build . --config [Release|Debug]

If Apache Ignite C++ is installed in default directories (i.e. /usr/local or /usr), setting IGNITE_CPP_DIR property
is not necessary. As a result executables will be in corresponding subdirectories in cmake-build-release directory.

For odbc-example additionaly ODBC Driver Manager must be present and installed on your platform and
Apache Ignite ODBC driver must be built and installed according to instructions for your platform.


Importing CMake projects to Visual Studio (tm) (since 2015):
------------------------------------------------------------
 Use CMakeSettings.json.in files in examples root directory as a template of real CMakeSettings.json.
 Edit it manually to set up correct environment variables and import CMake projects as usual.

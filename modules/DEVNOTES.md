# Setting up and building
## Prerequisites
 * Java 11 SDK
 * Maven 3.6.0+ (for building)
 
## Building Ignite
Ignite follows standard guidelines for multi-module maven projects, so it can be easily built using the following 
command from the project root directory:
```commandline
mvn clean install
```
You can disable the tests when building using the following command:
```commandline
mvn clean install -DskipTests
```

### Running tests
To run unit tests only, use the following command:
```commandline
mvn test
```

Before running integration tests, make sure to build the project using the ``package`` target.

To run unit + integration tests, use the following command:
```commandline
mvn integration-test
```

To run integration tests only, use the following command:
```commandline
mvn integration-test -Dskip.surefire.tests=true
``` 

## Setting up IntelliJ Idea project
You can quickly import Ignite project to your IDE using the root `pom.xml` file. In IntelliJ, choose `Open Project` 
from the `Quick Start` box or choose `Open...` from the `File` menu and select the root `pom.xml` file.

After opening the project in IntelliJ, double check that the Java SDK is properly configured for the project:

 * Open the `File` menu and select `Project Structure...`
 * In the SDKs section, ensure that a 1.11 JDK is selected (create one if none exist)
 * In the `Project` section, make sure the project language level is set to 11.0 as Ignite makes use of several Java 11 
 language features
 
Ignite uses machine code generation for some of it's modules. To generate necessary production code, build the project
using maven (see Building Ignite).

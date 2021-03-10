# Setting up and building
## Prerequisites
 * Java 11 SDK
 * Maven 3.6.0+ (for building)
 
## Building Ignite
Ignite follows standard guidelines for multi-module maven projects, so it can be easily built using the following 
command from the project root directory (you can disable the tests when building using `-DskipTests`):

    mvn clean install [-DskipTests]

Upon completion, you will find the CLI tool under `modules/cli/target` directory.
Use `ignite` on Linux and MacOS, or `ignite.exe` on Windows. 

### Running tests
To run unit tests only, use the following command:

    mvn test

Before running integration tests, make sure to build the project using the ``package`` target.

To run unit + integration tests, use the following command:

    mvn integration-test

To run integration tests only, use the following command:

    mvn integration-test -Dskip.surefire.tests=true
    
### Sanity check targets
Use the following commands to run generic sanity checks before submitting a PR.

RAT license validation:
    
    mvn validate
    
Checkstyle code validation:

    mvn checkstyle:checkstyle:aggregate

PMD static code analysis

    mvn compile pmd:check 


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

## Code structure
High-level modules structure and detailed modules description can be found in the [modules readme](modules/README.md).

## Release candidate verification

**1. Build the package (this will also run unit tests and the license headers check)**

    mvn clean package

**2. Go to the `modules/cli/target` directory which now contains the packaged CLI tool**

    cd modules/cli/target

**3. Run the tool without parameters (full list of available commands should appear)**

    ./ignite
    
**4. Run the initialization step**

    ./ignite init --repo=<path to Maven staging repository>

**5. Install an additional dependency (Guava is used as an example)**

    ./ignite module add mvn:com.google.guava:guava:23.0

**6. Verify that Guava has been installed**

    ./ignite module list

**7. Start a node**

    ./ignite node start myFirstNode

**8. Check that the new node is up and running**

    ./ignite node list
    
**9. Stop the node**

    ./ignite node stop myFirstNode

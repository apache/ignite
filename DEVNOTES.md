## Local Build

    mvn clean install [-DskipTests]

Upon completion, you will find the CLI tool under `modules/cli/target` directory.
Use `ignite` on Linux and MacOS, or `ignite.exe` on Windows. 

## License Headers Check

    mvn validate

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

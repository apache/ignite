## Maven Build Instructions

### Running full build (with or without unit tests)

    mvn clean install [-DskipTests]

Upon completion, you will find the CLI tool under `modules/cli/target` directory.
Use `ignite` on Linux and MacOS, or `ignite.exe` on Windows. 
    
### Checking license headers using Apache RAT

    mvn validate

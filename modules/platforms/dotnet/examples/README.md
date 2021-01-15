# TODO
* Populate this README
* Build examples as part of build.ps1, utilize NuGet.config to reference the latest version
  * This won't work during release verification
  * Use conditionals to reference dll from ../bin/netcoreapp
  * Use single Ignite reference in the Shared project  
* Use string interpolation everywhere
* Review cache names (derive cache name from the example name? - add this to template?)
* Review descriptions
* Write tests to verify descriptions, lack of TODOs, proper namespaces, what else?
* Write tests to run the examples (by loading the assemblies and executing the Main method?) 
* Add vscode json stuff and write tests for this

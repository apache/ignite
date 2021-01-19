# Ignite.NET Examples

## Requirements

* [.NET Core 2.1+](https://dotnet.microsoft.com/download/dotnet-core)
* [JDK 8](https://www.oracle.com/java/technologies/javase/javase-jdk8-downloads.html) or [JDK 11](https://www.oracle.com/java/technologies/javase-jdk11-downloads.html)

Windows, Linux, and macOS are supported.

## Command Line

* Change to a specific example directory: `cd Thick/Cache/PutGet`
* `dotnet run`

Thin Client examples require one or mode Ignite server node, run this in a separate terminal window before starting the example:
* `cd ServerNode`
* `dotnet run`

## Visual Studio

* Open `IgniteExamples.sln`
* Select an example on the Run toolbar (TODO: picture) and run

## VS Code

* Open current folder (from UI or with `code .` command)
* Open "Run" panel (`Ctrl+Shift+D` - default shortcut for `workbench.view.debug`)
* Select an example from the combobox on top and run

![VS Code Screenshot](images/vs-code.png)

## Rider

* Open `IgniteExamples.sln`
* Select an example on the Run toolbar and run

![Rider Toolbar Screenshot](images/rider.png)

* Alternatively, open the example source code an run it using the sidebar icon

![Rider Sidebar Screenshot](images/rider-sidebar.png)

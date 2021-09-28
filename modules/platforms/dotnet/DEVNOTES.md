## Prerequisites
* .NET Core 3.1 SDK
* Java 11 SDK
* Maven 3.6.0+ (for building)

## Build Java
In repo root: `mvn clean install -DskipTests`

## Build .NET
In this dir: `dotnet build`

## Run Tests
In this dir: `dotnet test`

## Start a Test Node
`mvn -Dtest=ITThinClientConnectionTest -DfailIfNoTests=false -DIGNITE_TEST_KEEP_NODES_RUNNING=true surefire:test`

## .NET Core 3.1 and .NET Standard 2.1

* Library project target `netstandard2.1`
* Test projects target `netcoreapp3.1`

See [IEP-78 .NET Thin Client](https://cwiki.apache.org/confluence/display/IGNITE/IEP-78+.NET+Thin+Client) for design considerations.

## Static Code Analysis

Static code analysis (Roslyn-based) runs as part of the build and includes code style check. Build fails on any warning.
* Analysis rules are defined in `Apache.Ignite.ruleset` and `Apache.Ignite.Tests.ruleset` (relaxed rule set for test projects).
* License header is defined in `stylecop.json`
* Warnings As Errors behavior is enabled in `Directory.Build.props` (can be disabled locally for rapid prototyping so that builds are faster and warnings don't distract)

## Release Procedure

### Build Binaries
`dotnet publish Apache.Ignite --configuration Release --output release/bin`

### Pack NuGet
`dotnet pack Apache.Ignite --configuration Release --include-source --output release/nupkg`

# Apache Ignite Project Templates

Templates for the `dotnet new` command.

## Public

`public` templates are published to NuGet for the end users

### Apache.Ignite.Examples

Template for the entire `../examples/Apache.Ignite.Examples.sln` solution - an easy way to deliver examples to the end user.
* Built as part of `build.ps1`
* Published to NuGet during the release
* Use: `dotnet new ignite-examples`

## Internal

`internal` templates are only used locally and not published

### Apache.Ignite.Example

Template for Server and Thick Client examples.

* Install: `dotnet new --install internal/Apache.Ignite.Example`
* Use: `dotnet new ignite-example`

### Apache.Ignite.ExampleThin

Template for Thin Client examples.

* Install: `dotnet new --install internal/Apache.Ignite.ExampleThin`
* Use: `dotnet new ignite-example-thin`

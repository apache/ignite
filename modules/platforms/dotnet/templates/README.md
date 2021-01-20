# Apache Ignite Project Templates

Templates for the `dotnet new` command.

## Public

`public` templates are published to NuGet for the end users

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

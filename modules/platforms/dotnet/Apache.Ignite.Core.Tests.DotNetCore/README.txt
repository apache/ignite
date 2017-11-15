Apache Ignite .NET Core tests (cross-platform)
==============================================

Main Apache.Ignite.sln solution targets .NET 4.0: we care for backwards compatibility.
However, this does not prevent us from supporting .NET Standard 2.0 and .NET Core 2.0,
because of ".NET Framework compatibility mode", which allows referencing any libraries
from .NET Core 2.0 projects.

Therefore we can't include .NET Core tests in manin solution, and we rely on pre-built
NuGet packages for cross-platform tests.

How to run:
1) Build Ignite.NET (only on Windows):  build -version 0.0.1
   Special 0.0.1 version override is used so that we don't have to change package reference in csproj file on each release.
2) Clear NuGet caches: dotnet nuget locals all --clear
3) Build and run cross-platform tests (any OS): dotnet test
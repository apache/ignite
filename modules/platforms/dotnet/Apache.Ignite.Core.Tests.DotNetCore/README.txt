Apache Ignite .NET Core tests (cross-platform)
==============================================

Main Apache.Ignite.sln solution targets .NET 4.0 & VS 2010: we care for backwards compatibility.
However, this does not prevent us from supporting .NET Standard 2.0 and .NET Core 2.0,
because of ".NET Framework compatibility mode", which allows referencing any libraries
from .NET Core 2.0 projects.

Therefore we can't include .NET Core tests in main solution,
and there is a separate Apache.Ignite.Core.DotNetCore solution and separate *DotNetCore projects.
Most tests are still reused from main solution using "Add As Link" feature.
rem Apache Ignite.NET build script

rem Requirements:
rem * PowerShell 3
rem * NuGet in PATH
rem * Apache Maven in PATH
rem * JDK 7+

rem Examples:
rem 'build -clean': Full rebuild of Java, .NET and NuGet packages.
rem 'build -skipJava -skipCodeAnalysis -skipNuGet -configuration Debug -platform x64': Quick build of .NET code only.

powershell -executionpolicy remotesigned -file build.ps1 %*
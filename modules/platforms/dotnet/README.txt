Apache Ignite.NET
==================================

Apache Ignite.NET provides a full featured .NET data grid and .NET compute grid functionality.
Using Apache Ignite.NET APIs you can execute any computation closure on the grid,
perform concurrent operations on the data stored in cache, start ACID transactions,
create distributed locks, subscribe for event listeners, etc.

Full source code is provided. Users should build the library for intended platform.

Common Requirements:

 * Microsoft Visual Studio (tm) 2010 or later
 * Microsoft Visual C++ 2010 Redistributable Package: http://www.microsoft.com/en-us/download/details.aspx?id=14632
 * Java Development Kit (JDK): https://java.com/en/download/index.jsp
 * JAVA_HOME environment variable must be set pointing to Java installation directory.

Building the library:
 * Open and build %IGNITE_HOME%\platforms\dotnet\Apache.Ignite.sln (or Apache.Ignite_x86.sln if you are running
   32-bit platform).

Development:
 * Add the library Apache.Ignite.Core.dll to your project references.
 * To start Apache Ignite as a standalone node or Windows service use Apache.Ignite.exe.
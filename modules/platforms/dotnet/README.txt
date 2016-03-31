Apache Ignite.NET
==================================

Apache Ignite.NET provides a full featured .NET data grid and .NET compute grid functionality.
Using Apache Ignite.NET APIs you can execute any computation closure on the grid,
perform concurrent operations on the data stored in cache, start ACID transactions,
create distributed locks, subscribe for event listeners, etc.

Files list:

 * Apache.Ignite.exe - executable to start standalone Ignite.NET node.
 * Apache.Ignite.Core.dll - Ignite.NET API library.
 * Apache.Ignite.Core.xml - Library XML documentation.


Development

 * Download and install Microsoft Visual Studio C# 2010 or later:
   https://www.visualstudio.com/
   
 * Download and install Microsoft Visual C++ 2010 Redistributable Package: 
   http://www.microsoft.com/en-us/download/details.aspx?id=14632
   
 * Download and install the latest Java Development Kit:
   https://java.com/en/download/index.jsp
 
 * Set JAVA_HOME environment variable as per this tutorial: 
   http://docs.oracle.com/cd/E19182-01/820-7851/inst_cli_jdk_javahome_t/index.html
 
 * Add Apache.Ignite.Core.dll to your project references (or x86\Apache.Ignite.Core.dll if you need 32-bit version).

 * To start Apache Ignite as a standalone node or Windows service use Apache.Ignite.exe.

Links

 * Documentation center: https://apacheignite-net.readme.io/
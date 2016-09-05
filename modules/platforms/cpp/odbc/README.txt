Apache Ignite ODBC driver
=======================================

Apache Ignite provides ODBC driver that can be used to retrieve distributed 
data from cache using standard SQL queries and native ODBC API.

For more info on ODBC please refer to ODBC Programmer's Reference at
https://msdn.microsoft.com/en-us/library/ms714177.aspx

To use Apache Ignite ODBC driver you first need to build and install it for
your system. You can find driver installation instructions below. For build
instruction please refer to $IGNITE_HOME/platforms/cpp/DEVNOTES.txt.

Connection string and DSN arguments
=======================================

Apache Ignite ODBC driver supports and uses following connection string/DSN
arguments:

1. Address of the node to connect to:
   SERVER=<host_name_or_IP_address>;
   
2. Port on which OdbcProcessor of the node is listening:
   PORT=<TCP_port>;
   
3. Datagrid cache to connect to:
   CACHE=<cache_name>;
   
All arguments are case-insensitive so "SERVER", "Server" and "server" all are
valid server address arguments.

Installing ODBC driver on Linux
=======================================

To be able to build and install ODBC driver on Linux you first need to install
ODBC Driver Manager. Apache Ignite ODBC driver has been tested with UnixODBC
(http://www.unixodbc.org). 

Once you have built and installed Ignite ODBC Driver i.e. libignite-odbc.so it
is most likely placed to /usr/local/lib. To install and be able to use Ignite
ODBC driver you should perfrom the following steps:

1. Ensure linker is able to locate all dependencies of the ODBC driver. You
   can check it using "ldd" command like this (assuming ODBC driver is located
   under /usr/local/lib):
   $ ldd /usr/local/lib/libignite-odbc.so.
   If there is unresolved links to other libraries you may want to add
   directories with these libraries to the LD_LIBRARY_PATH.
   
2. Edit file $IGNITE_HOME/platforms/cpp/odbc/install/ignite-odbc-install.ini
   and ensure that "Driver" parameter of the "Apache Ignite" section points
   to the right location where libignite-odbc.so is located.
   
3. To install Apache Ignite ODBC driver use the following command:
   $ odbcinst -i -d -f $IGNITE_HOME/platforms/cpp/odbc/install/ignite-odbc-install.ini
   To perform this command you most likely will need root privileges.

Installing ODBC driver on Windows
=======================================

For 32-bit Windows you should use 32-bit version of the driver while for the
64-bit Windows you can use 64-bit driver as well as 32-bit.

To install driver on Windows you should first choose a directory on your
filesystem where your driver or drivers will be located. Once you have
choosen the place you should put your driver there and ensure that all driver
dependencies can be resolved i.e. they can be found either in the %PATH% or
in the same directory as the driver.

After that you should use one of the install scripts from the directory 
%IGNITE_HOME%/platforms/cpp/odbc/install. Note that most likely you will
need OS administrator privileges to execute these scripts.

For the 32-bit Windows you should use file install_x86.cmd like that:
$ install_x86 <absolute_path_to_32_bit_driver>

For the 64-bit Windows you should use file install_amd64.cmd like that:
$ install_amd64 <absolute_path_to_64_bit_driver> [<absolute_path_to_32_bit_driver>]

Thats it. Your driver/drivers are installed.

After the installation
=======================================
   
Now Apache Ignite ODBC driver is installed and ready for use. You can connect
to it and use it like to any other ODBC driver.

For further instruction on the usage of the ODBC driver please refer to the
official ODBC documentation.

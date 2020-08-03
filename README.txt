Apache Ignite In-Memory Database and Caching Platform
=====================================================

Ignite is a memory-centric distributed database, caching, and processing platform for transactional, analytical,
and streaming workloads delivering in-memory speeds at petabyte scale.

The main feature set of Ignite includes:
* Memory-Centric Storage
* Advanced Clustering
* Distributed Key-Value
* Distributed SQL
* Compute Grid
* Service Grid
* Distributed Data Structures
* Distributed Messaging
* Distributed Events
* Streaming & CEP

For information on how to get started with Apache Ignite please visit:

    http://apacheignite.readme.io/docs/getting-started


You can find Apache Ignite documentation here:

    http://apacheignite.readme.io/docs


Crypto Notice
=============

This distribution includes cryptographic software. The country in
which you currently reside may have restrictions on the import, possession,
use, and/or re-export to another country, of encryption software.
BEFORE using any encryption software, please check your country's laws,
regulations and policies concerning the import, possession, or use,
and re-export of encryption software, to see if this is permitted.
See http://www.wassenaar.org/ for more information.

The Apache Software Foundation has classified this software as Export
Commodity Control Number (ECCN) 5D002, which includes information
security software using or performing cryptographic functions with
asymmetric algorithms. The form and manner of this Apache Software
Foundation distribution makes it eligible for export under the
"publicly available" Section 742.15(b) exemption (see the BIS Export
Administration Regulations, Section 742.15(b)) for both object code
and source code.

The following provides more details on the included cryptographic software:

* JDK SSL/TLS libraries used to enable secured connectivity between cluster
nodes (https://apacheignite.readme.io/docs/ssltls).
Oracle/OpenJDK (https://www.oracle.com/technetwork/java/javase/downloads/index.html)

* JDK Java Cryptography Extensions build in encryption from the Java libraries is used
for Transparent Data Encryption of data on disk
(https://apacheignite.readme.io/docs/transparent-data-encryption)
and for AWS S3 Client Side Encryprion.
(https://java.sun.com/javase/technologies/security/)

* Python client uses Python's SSL lib (https://docs.python.org/3/library/ssl.html) which is a wrapper over OpenSSL;
* NodeJS client uses NodeJS's TLS module (https://nodejs.org/api/tls.html) which is a wrapper over OpenSSL;
* PHP client uses PHP OpenSSL extension (https://www.php.net/manual/en/book.openssl.php);
* C++ thin client and ODBC use OpenSSL to establish secure connection with the cluster.
The OpenSSL Project (https://www.openssl.org/)

* Apache Ignite SSH module requires the JSch library. This provides capabilities to
start Apache Ignite nodes on remote machines via SSH.
JCraft (http://www.jcraft.com/jsch/)

* Apache Ignite REST http module requires Jetty, and this module can use HTTPs (uses SSL).
Eclipse Jetty (http://eclipse.org/jetty)

* Apache Ignite.NET uses .NET Framework crypto APIs from standard class library
for all security and cryptographic related code.
 .NET Classic, Windows-only (https://dotnet.microsoft.com/download)
 .NET Core  (https://dotnetfoundation.org/projects)
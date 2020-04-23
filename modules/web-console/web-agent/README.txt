Ignite Web Agent
======================================
Ignite Web Agent is a java standalone application that allow to connect Ignite Grid to Ignite Web Console.
Ignite Web Agent communicates with grid nodes via REST interface and connects to Ignite Web Console via web-socket.

Two main functions of Ignite Web Agent:
 1. Proxy between Ignite Web Console and Ignite Grid to execute SQL statements and collect metrics for monitoring.
   You may need to specify URI for connect to Ignite REST server via "-n" option.

 2. Proxy between Ignite Web Console and user RDBMS to collect database metadata for later indexed types configuration.
   You may need to copy JDBC driver into "./jdbc-drivers" subfolder or specify path via "-d" option.

Usage example:
  ignite-web-agent.sh

Configuration file:
  Should be a file with simple line-oriented format as described here: http://docs.oracle.com/javase/7/docs/api/java/util/Properties.html#load(java.io.Reader)

  Available entries names:
    tokens
    server-uri
    node-uri
    node-login
    node-password
    driver-folder
    node-key-store
    node-key-store-password
    node-trust-store
    node-trust-store-password
    server-key-store
    server-key-store-password
    server-trust-store
    server-trust-store-password
    cipher-suites

  Example configuration file:
    tokens=1a2b3c4d5f,2j1s134d12
    server-uri=https://console.example.com
    node-uri=http://10.0.0.1:8080,http://10.0.0.2:8080

Security tokens:
  1) By default security token of current user will be included into "default.properties" inside downloaded "ignite-web-agent-x.x.x.zip".
  2) One can get/reset token in Web Console profile (https://<your_console_address>/settings/profile).
  3) One may specify several comma-separated list of tokens using configuration file or command line arguments of web agent.

Ignite Web agent requirements:
  1) In order to communicate with web agent Ignite node should be started with REST server (copy "ignite-rest-http" folder from "libs/optional/" to "libs/").
  2) Configure web agent server-uri property with address where Web Console is running.
  3) Configure web agent node-uri property with Ignite nodes URI(s).

Options:
  -h, --help
    Print this help message
  -c, --config
    Path to agent property file
    Default value: default.properties
  -d, --driver-folder
    Path to folder with JDBC drivers
    Default value: ./jdbc-drivers
  -n, --node-uri
    Comma-separated list of URIs for connect to Ignite node via REST
    Default value: http://localhost:8080
  -nl, --node-login
    User name that will be used to connect to secured cluster
  -np, --node-password
    Password that will be used to connect to secured cluster
  -s, --server-uri
    URI for connect to Ignite Console via web-socket protocol
    Default value: http://localhost:3000
  -t, --tokens
     User's tokens separated by comma used to connect to Ignite Console.
  -nks, --node-key-store
    Path to key store that will be used to connect to cluster
  -nksp, --node-key-store-password
    Optional password for node key store
  -nts, --node-trust-store
    Path to trust store that will be used to connect to cluster
  -ntsp, --node-trust-store-password
    Optional password for node trust store
  -sks, --server-key-store
    Path to key store that will be used to connect to Web server
  -sksp, --server-key-store-password
    Optional password for server key store
  -sts, --server-trust-store
    Path to trust store that will be used to connect to Web server
  -stsp, --server-trust-store-password
    Optional password for server trust store
  -cs, --cipher-suites
     Optional comma-separated list of SSL cipher suites to be used to connect
     to server and cluster

How to build:
  To build from sources run following command in Ignite project root folder:
  mvn clean package -pl :ignite-web-agent -am -P web-console -DskipTests=true

Demo of Ignite Web Agent:
 In order to simplify evaluation demo mode was implemented. To start demo, you need to click button "Start demo".
 New tab will be open with prepared demo data.

 1) Demo for import domain model from database.
   In this mode an in-memory H2 database will be started.
   How to evaluate:
     1.1) Go to Ignite Web Console "Domain model" screen.
     1.2) Click "Import from database". You should see modal with demo description.
     1.3) Click "Next" button. You should see list of available schemas.
     1.4) Click "Next" button. You should see list of available tables.
     1.5) Click "Next" button. You should see import options.
     1.6) Select some of them and click "Save".

   2) Demo for SQL.
     How to evaluate:
     In this mode internal Ignite node will be started. Cache created and populated with data.
       2.1) Click "SQL" in Ignite Web Console top menu.
       2.2) "Demo" notebook with preconfigured queries will be opened.
       2.3) You can also execute any SQL queries for tables: "Country, Department, Employee, Parking, Car".

 For example:
   2.4) Enter SQL statement:
           SELECT p.name, count(*) AS cnt FROM "ParkingCache".Parking p
           INNER JOIN "CarCache".Car c ON (p.id) = (c.parkingId)
           GROUP BY P.NAME
   2.5) Click "Execute" button. You should get some data in table.
   2.6) Click charts buttons to see auto generated charts.

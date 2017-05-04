Ignite Web Agent
======================================
Ignite Web Agent is a java standalone application that allow to connect Ignite Grid to Ignite Web Console.
Ignite Web Agent communicates with grid nodes via REST interface and connects to Ignite Web Console via web-socket.

Two main functions of Ignite Web Agent:
 1. Proxy between Ignite Web Console and Ignite Grid to execute SQL statements and collect metrics for monitoring.
   You may need to specify URI for connect to Ignite REST server via "-n" option.

 2. Proxy between Ignite Web Console and user RDBMS to collect database metadata for later CacheTypeMetadata configuration.
   You may need to copy JDBC driver into "./jdbc-drivers" subfolder or specify path via "-d" option.

Usage example:
  ignite-web-agent.sh

Configuration file:
  Should be a file with simple line-oriented format as described here: http://docs.oracle.com/javase/7/docs/api/java/util/Properties.html#load(java.io.Reader)

  Available entries names:
    tokens
    server-uri
    node-uri
    driver-folder

  Example configuration file:
    tokens=1a2b3c4d5f,2j1s134d12
    serverURI=https://console.example.com

Security tokens:
  1) By default security token of current user will be included into "default.properties" inside downloaded "ignite-web-agent-x.x.x.zip".
  2) One can get/reset token in Web Console profile (https://<your_console_address>/settings/profile).
  3) One may specify several comma separated tokens using configuration file or command line arguments of web agent.

Ignite Web agent requirements:
  1) In order to communicate with web agent Ignite node should be started with REST server (copy "ignite-rest-http" folder from "libs/optional/" to "libs/").
  2) Configure web agent serverURI property by Ignite node REST server URI.

Options:
  -h, --help
     Print this help message
  -c, --config
     Path to configuration file
  -d, --driver-folder
     Path to folder with JDBC drivers, default value: ./jdbc-drivers
  -n, --node-uri
     URI for connect to Ignite REST server, default value:
     http://localhost:8080
  -s, --server-uri
     URI for connect to Ignite Web Console via web-socket protocol, default
     value: http://localhost:3000
  -t, --tokens
     User's security tokens

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
           SELECT p.name, count(*) AS cnt
           FROM "ParkingCache".Parking p
           INNER JOIN "CarCache".Car c
             ON (p.id) = (c.parkingId)
           GROUP BY P.NAME
   2.5) Click "Execute" button. You should get some data in table.
   2.6) Click charts buttons to see auto generated charts.


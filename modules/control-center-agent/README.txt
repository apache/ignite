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
    ignite-web-agent.sh -t 1a2b3c4d5f -s wss://console.example.com

Test drive of Ignite Web Agent:
    In order to simplify evaluation two test drive modes were implemented:

    1) Get security token on Web Console "Profile" screen.

    2) Test drive for metadata load from database. Activated by option: -tm or --test-drive-metadata.
       In this mode an in-memory H2 database will started.
       How to evaluate:
         2.1) Go to Ignite Web Console "Metadata" screen.
         2.2) Select "Load from database".
         2.3) Select H2 driver and enter JDBC URL: "jdbc:h2:mem:test-drive-db".
         2.4) You should see list of available schemas and tables. Select some of them and click "Save".

    3) Test drive for SQL. Activated by option: -ts or --test-drive-sql.
       In this mode internal Ignite node will be started. Cache created and populated with data.
       How to evaluate:
       3.1) Go to Ignite Web Console "SQL" menu and select "Create new notebook" menu item.
       3.2) In notebook paragraph enter SQL queries for tables: "Country, Department, Employee" in "test-drive-employee" cache
        and for tables: "Parking, Car" in "test-drive-car" cache.

       For example:
        3.3) select "test-drive-car" cache,
        3.4) enter SQL:
                select count(*) cnt, p.ParkingName from car c
                 inner join PARKING p on (p.PARKINGID=c.PARKINGID)
                group by c.PARKINGID order by p.ParkingName
        3.5) Click "Execute" button. You should get some data in table.
        3.6) Click charts buttons to see auto generated charts.

Configuration file:
    Should be a file with simple line-oriented format as described here: http://docs.oracle.com/javase/7/docs/api/java/util/Properties.html#load(java.io.Reader)

    Available entries names:
        token
        server-uri
        node-uri
        driver-folder
        test-drive-metadata
        test-drive-sql

    Example configuration file:
        token=1a2b3c4d5f
        serverURI=wss://console.example.com:3001
        test-drive-sql=true

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
       value: wss://localhost:3001
    -tm, --test-drive-metadata
       Start H2 database with sample tables in same process. JDBC URL for
       connecting to sample database: jdbc:h2:mem:test-drive-db
    -ts, --test-drive-sql
       Create cache and populate it with sample data for use in query
    -t, --token
       User's security token

Ignite Web Agent Build Instructions
==============================================
If you want to build from sources run following command in Ignite project root folder:
    mvn clean package -pl :ignite-control-center-agent -am -P control-center -DskipTests=true

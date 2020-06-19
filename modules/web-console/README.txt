Ignite Web Console
======================================
An Interactive Configuration Wizard and Management Tool for Apache Ignite

The Apache Ignite Web Console includes an interactive configuration wizard which helps you create and download configuration
 files for your Apache Ignite cluster. The tool also provides management capabilities which allow you to run SQL queries
 on your in-memory cache as well as view execution plans, in-memory schema, and streaming charts.

In order to simplify evaluation of Web Console demo mode was implemented.
 To start demo, you need to click button "Start demo". New tab will be open with prepared demo data on each screen.

 Demo for import domain model from database.
  In this mode an in-memory H2 database will be started.
  How to evaluate:
    1) Go to Ignite Web Console "Domain model" screen.
    2) Click "Import from database". You should see modal with demo description.
    3) Click "Next" button. You should see list of available schemas.
    4) Click "Next" button. You should see list of available tables.
    5) Click "Next" button. You should see import options.
    6) Select some of them and click "Save".

 Demo for SQL.
   How to evaluate:
    In this mode internal Ignite node will be started. Cache created and populated with data.
     1) Click "SQL" in Ignite Web Console top menu.
     2) "Demo" notebook with preconfigured queries will be opened.
     3) You can also execute any SQL queries for tables: "Country, Department, Employee, Parking, Car".

 For example:
  1) Enter SQL statement:
      SELECT p.name, count(*) AS cnt FROM "ParkingCache".Parking p
       INNER JOIN "CarCache".Car c ON (p.id) = (c.parkingId)
       GROUP BY P.NAME
  2) Click "Execute" button. You should get some data in table.
  3) Click charts buttons to see auto generated charts.


package org.apache.ignite.examples.spark;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.ignite.IgniteSparkSession;

import static org.apache.ignite.internal.util.typedef.X.println;

public class IgniteCatalogExample {
    /**
     * Ignite config file.
     */
    private static final String CONFIG = "examples/config/example-ignite.xml";

    /**
     * Test cache name.
     */
    private static final String CACHE_NAME = "testCache";

    public static void main(String args[]) throws AnalysisException {

        setupServerAndData();

        // Creating Ignite-specific implementation of Spark session.
        IgniteSparkSession igniteSession = IgniteSparkSession.builder()
                .appName("Spark Ignite catalog example")
                .master("local")
                .config("spark.executor.instances", "2")
                .igniteConfig(CONFIG)
                .getOrCreate();

        // Adjust the logger to exclude the logs of no interest.
        Logger.getRootLogger().setLevel(Level.ERROR);
        Logger.getLogger("org.apache.ignite").setLevel(Level.INFO);

        System.out.println("List of available tables:");

        // Showing existing tables.
        igniteSession.catalog().listTables().show();

        System.out.println("PERSON table description:");

        // Showing `person` schema.
        igniteSession.catalog().listColumns("person").show();

        System.out.println("CITY table description:");

        // Showing `city` schema.
        igniteSession.catalog().listColumns("city").show();

        println("Querying all persons from city with ID=2.");

        // Selecting data through Spark SQL engine.
        Dataset<Row> df = igniteSession.sql("SELECT * FROM person WHERE CITY_ID = 2");

        System.out.println("Result schema:");

        df.printSchema();

        System.out.println("Result content:");

        df.show();

        System.out.println("Querying all persons living in Denver.");

        // Selecting data through Spark SQL engine.
        Dataset<Row> df2 = igniteSession.sql("SELECT * FROM person p JOIN city c ON c.ID = p.CITY_ID WHERE c.NAME = 'Denver'");

        System.out.println("Result schema:");

        df2.printSchema();

        System.out.println("Result content:");

        df2.show();

        Ignition.stop(false);
    }

    private static void setupServerAndData() {
        // Starting Ignite.
        Ignite ignite = Ignition.start(CONFIG);

        // Creating cache.
        CacheConfiguration<?, ?> ccfg = new CacheConfiguration<>(CACHE_NAME).setSqlSchema("PUBLIC");

        IgniteCache<?, ?> cache = ignite.getOrCreateCache(ccfg);

        // Create tables.
        cache.query(new SqlFieldsQuery(
                "CREATE TABLE city (id LONG PRIMARY KEY, name VARCHAR) WITH \"template=replicated\"")).getAll();

        cache.query(new SqlFieldsQuery(
                "CREATE TABLE person (id LONG, name VARCHAR, city_id LONG, PRIMARY KEY (id, city_id)) " +
                        "WITH \"backups=1, affinityKey=city_id\"")).getAll();

        cache.query(new SqlFieldsQuery("CREATE INDEX on Person (city_id)")).getAll();

        // Inserting some data into table.
        SqlFieldsQuery qry = new SqlFieldsQuery("INSERT INTO city (id, name) VALUES (?, ?)");

        cache.query(qry.setArgs(1L, "Forest Hill")).getAll();
        cache.query(qry.setArgs(2L, "Denver")).getAll();
        cache.query(qry.setArgs(3L, "St. Petersburg")).getAll();

        qry = new SqlFieldsQuery("INSERT INTO person (id, name, city_id) values (?, ?, ?)");

        cache.query(qry.setArgs(1L, "John Doe", 3L)).getAll();
        cache.query(qry.setArgs(2L, "Jane Roe", 2L)).getAll();
        cache.query(qry.setArgs(3L, "Mary Major", 1L)).getAll();
        cache.query(qry.setArgs(4L, "Richard Miles", 2L)).getAll();
    }
}

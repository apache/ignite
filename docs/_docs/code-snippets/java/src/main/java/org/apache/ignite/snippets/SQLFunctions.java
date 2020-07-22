package org.apache.ignite.snippets;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.junit.jupiter.api.Test;

public class SQLFunctions {

    // tag::sql-function-example[]
    @QuerySqlFunction
    public static int sqr(int x) {
        return x * x;
    }
    // end::sql-function-example[]

    @Test
    IgniteCache setSqlFunction(Ignite ignite) {

        // tag::config[]
        // Preparing a cache configuration.
        CacheConfiguration cfg = new CacheConfiguration("myCache");

        // Registering the class that contains custom SQL functions.
        cfg.setSqlFunctionClasses(SQLFunctions.class);

        IgniteCache cache = ignite.createCache(cfg);
        // end::config[]

        return cache;
    }

    void call(IgniteCache cache) {

        // tag::query[]
        // Preparing the query that uses the custom defined 'sqr' function.
        SqlFieldsQuery query = new SqlFieldsQuery("SELECT name FROM myCache WHERE sqr(size) > 100");

        // Executing the query.
        cache.query(query).getAll();

        // end::query[]
    }

    public static void main(String[] args) {

        try (Ignite ignite = Ignition.start()) {
            SQLFunctions sqlf = new SQLFunctions();
            IgniteCache cache = sqlf.setSqlFunction(ignite);
            sqlf.call(cache);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}

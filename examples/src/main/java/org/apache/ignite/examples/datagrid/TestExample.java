package org.apache.ignite.examples.datagrid;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.examples.model.Person;

import java.util.List;

public class TestExample {
    public static final String CACHE_NAME = "cache";

    public static void main(String[] args) {
        try (Ignite srv = Ignition.start(new IgniteConfiguration().setLocalHost("127.0.0.1"))) {
            IgniteCache<Long, Person> srvCache = srv.createCache(
                new CacheConfiguration<Long, Person>().setName(CACHE_NAME).setIndexedTypes(Long.class, Person.class));

            for (long i = 0; i < 10_000; i++)
                srvCache.put(i, new Person(i, "First" + i, "Last" + i));

            System.out.println("PUT FINISHED");

            try (Ignite cli = Ignition.start(new IgniteConfiguration()
                .setIgniteInstanceName("client").setLocalHost("127.0.0.1").setClientMode(true))) {
                System.out.println("CLIENT STARTED");

                IgniteCache<Long, Person> cliCache = cli.cache(CACHE_NAME);

                SqlFieldsQuery qry = new SqlFieldsQuery("SELECT firstName FROM Person WHERE 1=1").setLazy(true);

                int cnt = 0;

                for (List<?> row : cliCache.query(qry)) {
                    cnt += row.size();

                    System.out.println("ROW: " + cnt);
                }

                System.out.println("DONE: " + cnt);
            }
            catch (Exception e) {
                System.out.println(e.toString());
            }
        }
    }
}

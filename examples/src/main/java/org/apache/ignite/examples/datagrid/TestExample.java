package org.apache.ignite.examples.datagrid;

import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.examples.model.Person;
public class TestExample {
    public static final String CACHE_NAME = "cache";
    public static void main(String[] args) {
        try (Ignite srv = Ignition.start()) {
            IgniteCache<Long, Person> srvCache = srv.createCache(
                new CacheConfiguration<Long, Person>().setName(CACHE_NAME).setIndexedTypes(Long.class, Person.class));
            for (long i = 0; i < 100; i++)
                srvCache.put(i, new Person(i, "First" + i, "Last" + i));
            try (Ignite cli=
                     Ignition.start(new IgniteConfiguration().setIgniteInstanceName("client").setClientMode(true))) {
                IgniteCache<Long, Person> cliCache = cli.cache(CACHE_NAME);
                SqlFieldsQuery qry = new SqlFieldsQuery("SELECT concat(firstName, 'x') FROM Person").setPageSize(3);
                int cnt = 0;
                for (List<?> row : cliCache.query(qry)) {
                    cnt += row.size();

                    System.out.println("ROW: " + row.get(0));
                }
                System.out.println("DONE: " + cnt);
            }
        }
    }
}
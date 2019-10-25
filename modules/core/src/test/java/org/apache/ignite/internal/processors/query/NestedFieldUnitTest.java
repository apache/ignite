import java.util.Collections;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.junit.Test;

import static org.junit.Assert.*;

public class NestedFieldUnitTest {
    @Test
    public void test1() throws Exception {
        doTest("\"Address.street\"");
    }
    @Test
    public void test2() throws Exception {
        doTest("\"address.street\"");
    }

    private void doTest(String streetColumnName) {
        TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi()
            .setIpFinder(new TcpDiscoveryVmIpFinder().setAddresses(Collections.singleton("127.0.0.1:47500..47509")));

        IgniteConfiguration cfg = new IgniteConfiguration().setDiscoverySpi(discoverySpi);

        try (Ignite ignite = Ignition.start(cfg)) {
            IgniteCache<Object, Object> cache = ignite.createCache(new CacheConfiguration<>("cache")
                .setIndexedTypes(Integer.class, Person.class));

            cache.put(1, new Person("john", new Address("baker", 221)));

            cache.query(new SqlFieldsQuery("alter table Person add column name varchar"));

            assertEquals("john", cache.query(new SqlFieldsQuery("select name from Person")).getAll().get(0).get(0));

//            cache.query(new SqlFieldsQuery("alter table Person add column address other"));
//
//            assertNotNull(cache.query(new SqlFieldsQuery("select address from Person")).getAll().get(0).get(0));

            cache.query(new SqlFieldsQuery("alter table Person add column " + streetColumnName + " varchar"));

            assertEquals("baker", cache.query(new SqlFieldsQuery("select " + streetColumnName + " from Person")).getAll().get(0).get(0));
        }
    }

    static class Person {
        String name;
        Address address;

        Person(String name, Address address) {
            this.name = name;
            this.address = address;
        }
    }

    static class Address {
        String street;
        int number;

        Address(String street, int number) {
            this.street = street;
            this.number = number;
        }
    }
}

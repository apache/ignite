/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.h2.sql;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Base set of queries to compare query results from h2 database instance and mixed ignite caches (replicated and partitioned)
 * which have the same data models and data content.
 */
public class BaseH2CompareQueryTest extends AbstractH2CompareQueryTest {
    /** Org count. */
    public static final int ORG_CNT = 30;

    /** Address count. */
    public static final int ADDR_CNT = 10;

    /** Person count. */
    public static final int PERS_CNT = 50;

    /** Product count. */
    public static final int PROD_CNT = 100;

    /** Purchase count. */
    public static final int PURCH_CNT = PROD_CNT * 5;

    /** */
    protected static final String ORG = "org";

    /** */
    protected static final String PERS = "pers";

    /** */
    protected static final String PURCH = "purch";

    /** */
    protected static final String PROD = "prod";

    /** */
    protected static final String ADDR = "addr";

    /** Cache org. */
    private static IgniteCache<Integer, Organization> cacheOrg;

    /** Cache pers. */
    private static IgniteCache<AffinityKey, Person> cachePers;

    /** Cache purch. */
    private static IgniteCache<AffinityKey, Purchase> cachePurch;

    /** Cache prod. */
    private static IgniteCache<Integer, Product> cacheProd;

    /** Cache address. */
    private static IgniteCache<Integer, Address> cacheAddr;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCacheConfiguration(
            cacheConfiguration(ORG, CacheMode.PARTITIONED, Integer.class, Organization.class),
            cacheConfiguration(PERS, CacheMode.PARTITIONED, AffinityKey.class, Person.class),
            cacheConfiguration(PURCH, CacheMode.PARTITIONED, AffinityKey.class, Purchase.class),
            cacheConfiguration(PROD, CacheMode.REPLICATED, Integer.class, Product.class),
            cacheConfiguration(ADDR, CacheMode.REPLICATED, Integer.class, Address.class));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        cacheOrg = null;
        cachePers = null;
        cachePurch = null;
        cacheProd = null;
        cacheAddr = null;
    }

    /** {@inheritDoc} */
    @Override protected void createCaches() {
        cacheOrg = jcache(ignite, cacheConfiguration(ORG, CacheMode.PARTITIONED, Integer.class, Organization.class), ORG, Integer.class, Organization.class);
        cachePers = ignite.cache(PERS);
        cachePurch = ignite.cache(PURCH);
        cacheProd = ignite.cache(PROD);
        cacheAddr = ignite.cache(ADDR);
    }

    /** {@inheritDoc} */
    @Override protected void initCacheAndDbData() throws SQLException {
        int idGen = 0;

        // Organizations.
        List<Organization> organizations = new ArrayList<>();

        for (int i = 0; i < ORG_CNT; i++) {
            int id = idGen++;

            Organization org = new Organization(id, "Org" + id);

            organizations.add(org);

            cacheOrg.put(org.id, org);

            insertInDb(org);
        }

        // Adresses.
        List<Address> addreses = new ArrayList<>();

        for (int i = 0; i < ADDR_CNT; i++) {
            int id = idGen++;

            Address addr = new Address(id, "Addr" + id);

            addreses.add(addr);

            cacheAddr.put(addr.id, addr);

            insertInDb(addr);
        }

        // Persons.
        List<Person> persons = new ArrayList<>();

        for (int i = 0; i < PERS_CNT; i++) {
            int id = idGen++;

            Person person = new Person(id, organizations.get(i % organizations.size()),
                "name" + id, "lastName" + id, id * 100.0, addreses.get(i % addreses.size()));

            // Add a Person without lastname.
            if (id == organizations.size() + 1)
                person.lastName = null;

            persons.add(person);

            cachePers.put(person.key(), person);

            insertInDb(person);
        }

        // Products.
        List<Product> products = new ArrayList<>();

        for (int i = 0; i < PROD_CNT; i++) {
            int id = idGen++;

            Product product = new Product(id, "Product" + id, id*1000);

            products.add(product);

            cacheProd.put(product.id, product);

            insertInDb(product);
        }

        // Purchases.
        for (int i = 0; i < PURCH_CNT; i++) {
            int id = idGen++;

            Person person = persons.get(i % persons.size());

            Purchase purchase = new Purchase(id, products.get(i % products.size()), person.orgId, person);

            cachePurch.put(purchase.key(), purchase);

            insertInDb(purchase);
        }
    }

    /** {@inheritDoc} */
    @Override protected void checkAllDataEquals() throws Exception {
        compareQueryRes0(cacheOrg, "select _key, _val, id, name from \"org\".Organization");

        compareQueryRes0(cachePers, "select _key, _val, id, firstName, lastName, orgId, salary from \"pers\".Person");

        compareQueryRes0(cachePurch, "select _key, _val, id, personId, productId, organizationId from \"purch\".Purchase");

        compareQueryRes0(cacheProd, "select _key, _val, id, name, price from \"prod\".Product");
    }

    /**
     *
     */
    @Test
    public void testSelectStar() {
        assertEquals(1, cachePers.query(new SqlQuery<AffinityKey<?>,Person>(
            Person.class, "\t\r\n  select  \n*\t from Person limit 1")).getAll().size());

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                cachePers.query(new SqlQuery(Person.class, "SELECT firstName from PERSON"));

                return null;
            }
        }, CacheException.class, null);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInvalidQuery() throws Exception {
        final SqlFieldsQuery sql = new SqlFieldsQuery("SELECT firstName from Person where id <> ? and orgId <> ?");

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                cachePers.query(sql.setArgs(3));

                return null;
            }
        }, IgniteException.class, "Invalid number of query parameters.");
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-705")
    @Test
    public void testAllExamples() throws Exception {
//        compareQueryRes0("select ? limit ? offset ?");

//        compareQueryRes0("select cool1()");
//        compareQueryRes0("select cool1() z");
//
//        compareQueryRes0("select b,a from table0('aaa', 100)");
//        compareQueryRes0("select * from table0('aaa', 100)");
//        compareQueryRes0("select * from table0('aaa', 100) t0");
//        compareQueryRes0("select x.a, y.b from table0('aaa', 100) x natural join table0('bbb', 100) y");
//        compareQueryRes0("select * from table0('aaa', 100) x join table0('bbb', 100) y on x.a=y.a and x.b = 'bbb'");
//        compareQueryRes0("select * from table0('aaa', 100) x left join table0('bbb', 100) y on x.a=y.a and x.b = 'bbb'");
//        compareQueryRes0("select * from table0('aaa', 100) x left join table0('bbb', 100) y on x.a=y.a where x.b = 'bbb'");
//        compareQueryRes0("select * from table0('aaa', 100) x left join table0('bbb', 100) y where x.b = 'bbb'");

        final String addStreet = "Addr" + ORG_CNT + 1;

        List<List<?>> res = compareQueryRes0(cachePers,
            "select avg(old) from \"pers\".Person left join \"addr\".Address " +
                " on Person.addrId = Address.id where lower(Address.street) = lower(?)", addStreet);

        assertNotSame(0, res);

        compareQueryRes0(cachePers,
            "select avg(old) from \"pers\".Person join \"addr\".Address on Person.addrId = Address.id " +
                "where lower(Address.street) = lower(?)", addStreet);

        compareQueryRes0(cachePers,
            "select avg(old) from \"pers\".Person left join \"addr\".Address " +
                "where Person.addrId = Address.id " +
                "and lower(Address.street) = lower(?)", addStreet);

        compareQueryRes0(cachePers,
            "select avg(old) from \"pers\".Person, \"addr\".Address where Person.addrId = Address.id " +
                "and lower(Address.street) = lower(?)", addStreet);

        compareQueryRes0(cachePers, "select firstName, date from \"pers\".Person");
        compareQueryRes0(cachePers, "select distinct firstName, date from \"pers\".Person");

        final String star = " _key, _val, id, firstName, lastName, orgId, salary, addrId, old, date ";

        compareQueryRes0(cachePers, "select " + star + " from \"pers\".Person p");
        compareQueryRes0(cachePers, "select " + star + " from \"pers\".Person");
        compareQueryRes0(cachePers, "select distinct " + star + " from \"pers\".Person");
        compareQueryRes0(cachePers, "select p.firstName, date from \"pers\".Person p");

        compareQueryRes0(cachePers, "select p._key, p._val, p.id, p.firstName, p.lastName, " +
            "p.orgId, p.salary, p.addrId, p.old, " +
            " p.date, a._key, a._val, a.id, a.street" +
            " from \"pers\".Person p, \"addr\".Address a");
//        compareQueryRes0("select p.* from \"part\".Person p, \"repl\".Address a");
//        compareQueryRes0("select person.* from \"part\".Person, \"repl\".Address a");
//        compareQueryRes0("select p.*, street from \"part\".Person p, \"repl\".Address a");
        compareQueryRes0(cachePers, "select p.firstName, a.street from \"pers\".Person p, \"addr\".Address a");
        compareQueryRes0(cachePers, "select distinct p.firstName, a.street from \"pers\".Person p, " +
            "\"addr\".Address a");
        compareQueryRes0(cachePers, "select distinct firstName, street from \"pers\".Person, " +
            "\"addr\".Address group by firstName, street ");
        compareQueryRes0(cachePers, "select distinct firstName, street from \"pers\".Person, " +
            "\"addr\".Address");
        // TODO uncomment and investigate (Rows count has to be equal.: Expected :2500, Actual :900)
//        compareQueryRes0("select p1.firstName, a2.street from \"part\".Person p1, \"repl\".Address a1, \"part\".Person p2, \"repl\".Address a2");

        //TODO look at it (org.h2.jdbc.JdbcSQLException: Feature not supported: "VARCHAR +" // at H2)
//        compareQueryRes0("select p.firstName n, a.street s from \"part\".Person p, \"repl\".Address a");
        compareQueryRes0(cachePers, "select p.firstName, 1 as i, 'aaa' s from \"pers\".Person p");

//        compareQueryRes0("select p.firstName + 'a', 1 * 3 as i, 'aaa' s, -p.old, -p.old as old from \"part\".Person p");
//        compareQueryRes0("select p.firstName || 'a' + p.firstName, (p.old * 3) % p.old - p.old / p.old, p.firstName = 'aaa', " +
//            " p.firstName is p.firstName, p.old > 0, p.old >= 0, p.old < 0, p.old <= 0, p.old <> 0, p.old is not p.old, " +
//            " p.old is null, p.old is not null " +
//            " from \"part\".Person p");

        compareQueryRes0(cachePers, "select p.firstName from \"pers\".Person p where firstName <> 'ivan'");
        compareQueryRes0(cachePers, "select p.firstName from \"pers\".Person p where firstName like 'i%'");
        compareQueryRes0(cachePers, "select p.firstName from \"pers\".Person p where firstName regexp 'i%'");
        compareQueryRes0(cachePers, "select p.firstName from \"pers\".Person p, \"addr\".Address a " +
            "where p.firstName <> 'ivan' and a.id > 10 or not (a.id = 100)");

        compareQueryRes0(cachePers, "select case p.firstName " +
            "when 'a' then 1 when 'a' then 2 end as a from \"pers\".Person p");
        compareQueryRes0(cachePers, "select case p.firstName " +
            "when 'a' then 1 when 'a' then 2 else -1 end as a from \"pers\".Person p");

        compareQueryRes0(cachePers, "select abs(p.old) from \"pers\".Person p");
        compareQueryRes0(cachePers, "select cast(p.old as numeric(10, 2)) from \"pers\".Person p");
        compareQueryRes0(cachePers, "select cast(p.old as numeric(10, 2)) z from \"pers\".Person p");
        compareQueryRes0(cachePers, "select cast(p.old as numeric(10, 2)) as z from \"pers\".Person p");

        // TODO analyse
//        compareQueryRes0("select " + star + " from \"part\".Person p where p.firstName in ('a', 'b', '_' + RAND())"); // test ConditionIn
        compareQueryRes0(cachePers, "select " + star + " from \"pers\".Person p where p.firstName in ('a', 'b', 'c')"); // test ConditionInConstantSet
        compareQueryRes0(cachePers, "select " + star + " from \"pers\".Person p " +
            "where p.firstName in (select a.street from \"addr\".Address a)"); // test ConditionInConstantSet

        compareQueryRes0(cachePers, "select (select a.street from \"addr\".Address a " +
            "where a.id = p.addrId) from \"pers\".Person p"); // test ConditionInConstantSet

        compareQueryRes0(cachePers, "select p.firstName, ? from \"pers\".Person p " +
            "where firstName regexp ? and p.old < ?", 10, "Iv*n", 40);

        compareQueryRes0(cachePers, "select count(*) as a from \"pers\".Person");
        compareQueryRes0(cachePers, "select count(*) as a, count(p.*), count(p.firstName) " +
            "from \"pers\".Person p");
        compareQueryRes0(cachePers, "select count(distinct p.firstName) " +
            "from \"pers\".Person p");

        compareQueryRes0(cachePers, "select p.firstName, avg(p.old), max(p.old) " +
            "from \"pers\".Person p group by p.firstName");
        compareQueryRes0(cachePers, "select p.firstName n, avg(p.old) a, max(p.old) m " +
            "from \"pers\".Person p group by p.firstName");
        compareQueryRes0(cachePers, "select p.firstName n, avg(p.old) a, max(p.old) m " +
            "from \"pers\".Person p group by n");

        compareQueryRes0(cachePers, "select p.firstName n, avg(p.old) a, max(p.old) m " +
            "from \"pers\".Person p group by p.addrId, p.firstName");
        compareQueryRes0(cachePers, "select p.firstName n, avg(p.old) a, max(p.old) m " +
            "from \"pers\".Person p group by p.firstName, p.addrId");
        compareQueryRes0(cachePers, "select p.firstName n, max(p.old) + min(p.old) / count(distinct p.old) " +
            "from \"pers\".Person p group by p.firstName");
        compareQueryRes0(cachePers, "select p.firstName n, max(p.old) maxOld, min(p.old) minOld " +
            "from \"pers\".Person p group by p.firstName having maxOld > 10 and min(p.old) < 1");

        compareQueryRes0(cachePers, "select p.firstName n, avg(p.old) a, max(p.old) m " +
            "from \"pers\".Person p group by p.firstName order by n");
        compareQueryRes0(cachePers, "select p.firstName n, avg(p.old) a, max(p.old) m " +
            "from \"pers\".Person p group by p.firstName order by p.firstName");
        compareQueryRes0(cachePers, "select p.firstName n, avg(p.old) a, max(p.old) m " +
            "from \"pers\".Person p group by p.firstName order by p.firstName, m");
        compareQueryRes0(cachePers, "select p.firstName n, avg(p.old) a, max(p.old) m " +
            "from \"pers\".Person p group by p.firstName order by p.firstName, max(p.old) desc");
        compareQueryRes0(cachePers, "select p.firstName n, avg(p.old) a, max(p.old) m " +
            "from \"pers\".Person p group by p.firstName order by p.firstName nulls first");
        compareQueryRes0(cachePers, "select p.firstName n, avg(p.old) a, max(p.old) m " +
            "from \"pers\".Person p group by p.firstName order by p.firstName nulls last");
        compareQueryRes0(cachePers, "select p.firstName n from \"pers\".Person p order by p.old + 10");
        compareQueryRes0(cachePers, "select p.firstName n from \"pers\".Person p " +
            "order by p.old + 10, p.firstName");
        compareQueryRes0(cachePers, "select p.firstName n from \"pers\".Person p " +
            "order by p.old + 10, p.firstName desc");

        compareQueryRes0(cachePers, "select p.firstName n from \"pers\".Person p, " +
            "(select a.street from \"addr\".Address a where a.street is not null)");
        compareQueryRes0(cachePers, "select street from \"pers\".Person p, " +
            "(select a.street from \"addr\".Address a where a.street is not null) ");
        compareQueryRes0(cachePers, "select addr.street from \"pers\".Person p, " +
            "(select a.street from \"addr\".Address a where a.street is not null) addr");

        compareQueryRes0(cachePers, "select p.firstName n from \"pers\".Person p order by p.old + 10");

        compareQueryRes0(cachePers, "select 'foo' as bar union select 'foo' as bar");
        compareQueryRes0(cachePers, "select 'foo' as bar union all select 'foo' as bar");

//        compareQueryRes0("select count(*) as a from Person union select count(*) as a from Address");
//        compareQueryRes0("select old, count(*) as a from Person group by old union select 1, count(*) as a from Address");
//        compareQueryRes0("select name from Person MINUS select street from Address");
//        compareQueryRes0("select name from Person EXCEPT select street from Address");
//        compareQueryRes0("select name from Person INTERSECT select street from Address");
//        compareQueryRes0("select name from Person UNION select street from Address limit 5");
//        compareQueryRes0("select name from Person UNION select street from Address limit ?");
//        compareQueryRes0("select name from Person UNION select street from Address limit ? offset ?");
//        compareQueryRes0("(select name from Person limit 4) UNION (select street from Address limit 1) limit ? offset ?");
//        compareQueryRes0("(select 2 a) union all (select 1) order by 1");
//        compareQueryRes0("(select 2 a) union all (select 1) order by a desc nulls first limit ? offset ?");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testParamSubstitution() throws Exception {
        compareQueryRes0(cachePers, "select ? from \"pers\".Person", "Some arg");
    }

    /**
     * @throws SQLException If failed.
     */
    @Test
    public void testAggregateOrderBy() throws SQLException {
        compareOrderedQueryRes0(cachePers, "select firstName name, count(*) cnt from \"pers\".Person " +
            "group by name order by cnt, name desc");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNullParamSubstitution() throws Exception {
        List<List<?>> rs1 = compareQueryRes0(cachePers, "select ? from \"pers\".Person", null);

        // Ensure we find something.
        assertFalse(rs1.isEmpty());
    }

    /**
     *
     */
    @Test
    public void testUnion() throws SQLException {
        String base = "select _val v from \"pers\".Person";

        compareQueryRes0(cachePers, base + " union all " + base);
        compareQueryRes0(cachePers, base + " union " + base);

        base = "select firstName||lastName name, salary from \"pers\".Person";

        assertEquals(PERS_CNT * 2, compareOrderedQueryRes0(cachePers, base + " union all " + base + " order by salary desc").size());
        assertEquals(PERS_CNT, compareOrderedQueryRes0(cachePers, base + " union " + base + " order by salary desc").size());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testEmptyResult() throws Exception {
        compareQueryRes0(cachePers, "select id from \"pers\".Person where 0 = 1");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSqlQueryWithAggregation() throws Exception {
        compareQueryRes0(cachePers, "select avg(salary) from \"pers\".Person, \"org\".Organization " +
            "where Person.orgId = Organization.id and " +
            "lower(Organization.name) = lower(?)", "Org1");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSqlFieldsQuery() throws Exception {
        compareQueryRes0(cachePers, "select concat(firstName, ' ', lastName) from \"pers\".Person");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSqlFieldsQueryWithJoin() throws Exception {
        compareQueryRes0(cachePers, "select concat(firstName, ' ', lastName), "
            + "Organization.name from \"pers\".Person, \"org\".Organization where "
            + "Person.orgId = Organization.id");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOrdered() throws Exception {
        compareOrderedQueryRes0(cachePers, "select firstName, lastName" +
            " from \"pers\".Person" +
            " order by lastName, firstName");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSimpleJoin() throws Exception {
        // Have expected results.
        compareQueryRes0(cachePers, String.format("select id, firstName, lastName" +
            "  from \"%s\".Person" +
            "  where Person.id = ?", cachePers.getName()), 3);

        // Ignite cache return 0 results...
        compareQueryRes0(cachePers, "select pe.firstName" +
            "  from \"pers\".Person pe join \"purch\".Purchase pu on pe.id = pu.personId " +
            "  where pe.id = ?", 3);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSimpleReplicatedSelect() throws Exception {
        compareQueryRes0(cacheProd, "select id, name from \"prod\".Product");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCrossCache() throws Exception {
        compareQueryRes0(cachePers, "select firstName, lastName" +
            "  from \"pers\".Person, \"purch\".Purchase" +
            "  where Person.id = Purchase.personId");

        compareQueryRes0(cachePers, "select concat(firstName, ' ', lastName), Product.name " +
            "  from \"pers\".Person, \"purch\".Purchase, \"prod\".Product " +
            "  where Person.id = Purchase.personId and Purchase.productId = Product.id" +
            "  group by Product.id");

        compareQueryRes0(cachePers, "select concat(firstName, ' ', lastName), count (Product.id) " +
            "  from \"pers\".Person, \"purch\".Purchase, \"prod\".Product " +
            "  where Person.id = Purchase.personId and Purchase.productId = Product.id" +
            "  group by Product.id");
    }

    /** {@inheritDoc} */
    @Override protected Statement initializeH2Schema() throws SQLException {
        Statement st = super.initializeH2Schema();

        st.execute("CREATE SCHEMA \"org\"");
        st.execute("CREATE SCHEMA \"pers\"");
        st.execute("CREATE SCHEMA \"prod\"");
        st.execute("CREATE SCHEMA \"purch\"");
        st.execute("CREATE SCHEMA \"addr\"");

        st.execute("create table \"org\".ORGANIZATION" +
            "  (_key int not null," +
            "  _val other not null," +
            "  id int unique," +
            "  name varchar(255))");

        st.execute("create table \"pers\".PERSON" +
            "  (_key other not null ," +
            "   _val other not null ," +
            "  id int unique, " +
            "  firstName varchar(255), " +
            "  lastName varchar(255)," +
            "  orgId int not null," +
            "  salary double," +
            "  addrId int," +
            "  old int," +
            "  date Date )");

        st.execute("create table \"prod\".PRODUCT" +
            "  (_key int not null ," +
            "   _val other not null ," +
            "  id int unique, " +
            "  name varchar(255), " +
            "  price int)");

        st.execute("create table \"purch\".PURCHASE" +
            "  (_key other not null ," +
            "   _val other not null ," +
            "  id int unique, " +
            "  personId int, " +
            "  organizationId int, " +
            "  productId int)");

        st.execute("create table \"addr\".ADDRESS" +
            "  (_key int not null ," +
            "   _val other not null ," +
            "  id int unique, " +
            "  street varchar(255))");

        conn.commit();

        return st;
    }

    /**
     * Insert {@link Organization} at h2 database.
     *
     * @param org Organization.
     * @throws SQLException If exception.
     */
    private void insertInDb(Organization org) throws SQLException {
        try(PreparedStatement st = conn.prepareStatement(
            "insert into \"org\".ORGANIZATION (_key, _val, id, name) values(?, ?, ?, ?)")) {
            st.setObject(1, org.id);
            st.setObject(2, org);
            st.setObject(3, org.id);
            st.setObject(4, org.name);

            st.executeUpdate();
        }
    }

    /**
     * Insert {@link Person} at h2 database.
     *
     * @param p Person.
     * @throws SQLException If exception.
     */
    private void insertInDb(Person p) throws SQLException {
        try(PreparedStatement st = conn.prepareStatement("insert into \"pers\".PERSON " +
            "(_key, _val, id, firstName, lastName, orgId, salary, addrId, old, date) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
            st.setObject(1, p.key());
            st.setObject(2, p);
            st.setObject(3, p.id);
            st.setObject(4, p.firstName);
            st.setObject(5, p.lastName);
            st.setObject(6, p.orgId);
            st.setObject(7, p.salary);
            st.setObject(8, p.addrId);
            st.setObject(9, p.old);
            st.setObject(10, p.date);

            st.executeUpdate();
        }
    }

    /**
     * Insert {@link Product} at h2 database.
     *
     * @param p Product.
     * @throws SQLException If exception.
     */
    private void insertInDb(Product p) throws SQLException {
        try(PreparedStatement st = conn.prepareStatement(
            "insert into \"prod\".PRODUCT (_key, _val, id, name, price) values(?, ?, ?, ?, ?)")) {
            st.setObject(1, p.id);
            st.setObject(2, p);
            st.setObject(3, p.id);
            st.setObject(4, p.name);
            st.setObject(5, p.price);

            st.executeUpdate();
        }
    }

    /**
     * Insert {@link Purchase} at h2 database.
     *
     * @param p Purchase.
     * @throws SQLException If exception.
     */
    private void insertInDb(Purchase p) throws SQLException {
        try(PreparedStatement st = conn.prepareStatement(
            "insert into \"purch\".PURCHASE (_key, _val, id, personId, productId, organizationId) values(?, ?, ?, ?, ?, ?)")) {
            st.setObject(1, p.key());
            st.setObject(2, p);
            st.setObject(3, p.id);
            st.setObject(4, p.personId);
            st.setObject(5, p.productId);
            st.setObject(6, p.organizationId);

            st.executeUpdate();
        }
    }

    /**
     * Insert {@link Address} at h2 database.
     *
     * @param a Address.
     * @throws SQLException If exception.
     */
    private void insertInDb(Address a) throws SQLException {
        try(PreparedStatement st = conn.prepareStatement(
            "insert into \"addr\".ADDRESS (_key, _val, id, street) values(?, ?, ?, ?)")) {
            st.setObject(1, a.id);
            st.setObject(2, a);
            st.setObject(3, a.id);
            st.setObject(4, a.street);

            st.executeUpdate();
        }
    }

    @QuerySqlFunction
    public static int cool1() {
        return 1;
    }

    @QuerySqlFunction
    public static ResultSet table0(Connection c, String a, int b) throws SQLException {
        return c.createStatement().executeQuery("select '" + a + "' as a, " +  b + " as b");
    }

    /**
     * Person class. Stored at partitioned cache.
     */
    private static class Person implements Serializable {
        /** Person ID (indexed). */
        @QuerySqlField(index = true)
        private int id;

        /** Organization ID (indexed). */
        @QuerySqlField(index = true)
        private int orgId;

        /** First name (not-indexed). */
        @QuerySqlField
        private String firstName;

        /** Last name (not indexed). */
        @QuerySqlField
        private String lastName;

        /** Salary (indexed). */
        @QuerySqlField(index = true)
        private double salary;

        /** Address Id (indexed). */
        @QuerySqlField(index = true)
        private int addrId;

        /** Date. */
        @QuerySqlField(index = true)
        public Date date = new Date(System.currentTimeMillis());

        /** Old. */
        @QuerySqlField(index = true)
        public int old = 17;


        /**
         * Constructs person record.
         *
         * @param org Organization.
         * @param firstName First name.
         * @param lastName Last name.
         * @param salary Salary.
         */
        Person(int id, Organization org, String firstName, String lastName, double salary, Address addr) {
            this.id = id;
            this.firstName = firstName;
            this.lastName = lastName;
            this.salary = salary;
            orgId = org.id;
            addrId = addr.id;
        }

        /**
         * @return Custom affinity key to guarantee that person is always collocated with organization.
         */
        public AffinityKey<Integer> key() {
            return new AffinityKey<>(id, orgId);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            return this == o || o instanceof Person && id == ((Person)o).id;

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return id;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Person [firstName=" + firstName +
                ", lastName=" + lastName +
                ", id=" + id +
                ", orgId=" + orgId +
                ", salary=" + salary +
                ", addrId=" + addrId + ']';
        }
    }

    /**
     * Organization class. Stored at partitioned cache.
     */
    private static class Organization implements Serializable {
        /** Organization ID (indexed). */
        @QuerySqlField(index = true)
        private int id;

        /** Organization name (indexed). */
        @QuerySqlField(index = true)
        private String name;

        /**
         * Create Organization.
         *
         * @param id Organization ID.
         * @param name Organization name.
         */
        Organization(int id, String name) {
            this.id = id;
            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            return this == o || o instanceof Organization && id == ((Organization)o).id;

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return id;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Organization [id=" + id + ", name=" + name + ']';
        }
    }

    /**
     * Product class. Stored at replicated cache.
     */
    private static class Product implements Serializable {
        /** Primary key. */
        @QuerySqlField(index = true)
        private int id;

        /** Product name. */
        @QuerySqlField
        private String name;

        /** Product price */
        @QuerySqlField
        private int price;

        /**
         * Create Product.
         *
         * @param id Product ID.
         * @param name Product name.
         * @param price Product price.
         */
        Product(int id, String name, int price) {
            this.id = id;
            this.name = name;
            this.price = price;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            return this == o || o instanceof Product && id == ((Product)o).id;

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return id;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Product [id=" + id + ", name=" + name + ", price=" + price + ']';
        }
    }

    /**
     * Purchase class. Stored at partitioned cache.
     */
    private static class Purchase implements Serializable {
        /** Primary key. */
        @QuerySqlField(index = true)
        private int id;

        /** Product ID. */
        @QuerySqlField
        private int productId;

        /** Person ID. */
        @QuerySqlField
        private int personId;

        /** Organization id. */
        @QuerySqlField
        private int organizationId;

        /**
         * Create Purchase.
         *  @param id Purchase ID.
         * @param product Purchase product.
         * @param organizationId Organization Id.
         * @param person Purchase person.
         */
        Purchase(int id, Product product, int organizationId, Person person) {
            this.id = id;
            productId = product.id;
            personId = person.id;
            this.organizationId = organizationId;
        }

        /**
         * @return Custom affinity key to guarantee that purchase is always collocated with person.
         */
        public AffinityKey<Integer> key() {
            return new AffinityKey<>(id, organizationId);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            return this == o || o instanceof Purchase && id == ((Purchase)o).id;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return id;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Purchase [id=" + id + ", productId=" + productId + ", personId=" + personId + ']';
        }
    }

    /**
     * Address class. Stored at replicated cache.
     */
    private static class Address implements Serializable {
        @QuerySqlField(index = true)
        private int id;

        @QuerySqlField(index = true)
        private String street;

        Address(int id, String street) {
            this.id = id;
            this.street = street;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            return this == o || o instanceof Address && id == ((Address)o).id;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return id;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Address [id=" + id + ", street=" + street + ']';
        }
    }
}

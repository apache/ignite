/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.query.h2.sql;

import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.query.*;
import org.gridgain.grid.kernal.processors.query.h2.*;
import org.h2.command.*;
import org.h2.command.dml.*;
import org.h2.engine.*;
import org.h2.jdbc.*;

import java.io.*;
import java.util.*;

/**
 *
 */
public class GridQueryTest extends GridCacheAbstractQuerySelfTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheMode cacheMode() {
        return GridCacheMode.REPLICATED;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        GridCache cache = ignite.cache(null);

        cache.putx("testAddr", new Address());
        cache.putx("testPerson", new Person());
    }

    /**
     *
     */
    public void testAllExampless() throws Exception {
        checkQuery("select name, date from Person");
        checkQuery("select distinct name, date from Person");
        checkQuery("select * from Person p");
        checkQuery("select * from Person");
        checkQuery("select distinct * from Person");
        checkQuery("select p.name, date from Person p");

        checkQuery("select * from Person p, Address a");
        checkQuery("select * from Person, Address");
        checkQuery("select p.* from Person p, Address a");
        checkQuery("select person.* from Person, Address a");
        checkQuery("select p.*, street from Person p, Address a");
        checkQuery("select p.name, a.street from Person p, Address a");
        checkQuery("select distinct p.name, a.street from Person p, Address a");
        checkQuery("select distinct name, street from Person, Address");
        checkQuery("select p1.name, a2.street from Person p1, Address a1, Person p2, Address a2");

        checkQuery("select p.name n, a.street s from Person p, Address a");
        checkQuery("select p.name, 1 as i, 'aaa' s from Person p");

        checkQuery("select p.name + 'a', 1 * 3 as i, 'aaa' s, -p.old, -p.old as old from Person p");
        checkQuery("select p.name || 'a' + p.name, (p.old * 3) % p.old - p.old / p.old, p.name = 'aaa', " +
            " p.name is p.name, p.old > 0, p.old >= 0, p.old < 0, p.old <= 0, p.old <> 0, p.old is not p.old, " +
            " p.old is null, p.old is not null " +
            " from Person p");

        checkQuery("select p.name from Person p where name <> 'ivan'");
        checkQuery("select p.name from Person p where name like 'i%'");
        checkQuery("select p.name from Person p where name regexp 'i%'");
        checkQuery("select p.name from Person p, Address a where p.name <> 'ivan' and a.id > 10 or not (a.id = 100)");

        checkQuery("select case p.name when 'a' then 1 when 'a' then 2 end as a from Person p");
        checkQuery("select case p.name when 'a' then 1 when 'a' then 2 else -1 end as a from Person p");

        checkQuery("select abs(p.old)  from Person p");
        checkQuery("select cast(p.old as numeric(10, 2)) from Person p");
        checkQuery("select cast(p.old as numeric(10, 2)) z from Person p");
        checkQuery("select cast(p.old as numeric(10, 2)) as z from Person p");

        checkQuery("select * from Person p where p.name in ('a', 'b', '_' + RAND())"); // test ConditionIn
        checkQuery("select * from Person p where p.name in ('a', 'b', 'c')"); // test ConditionInConstantSet
        checkQuery("select * from Person p where p.name in (select a.street from Address a)"); // test ConditionInConstantSet

        checkQuery("select (select a.street from Address a where a.id = p.addrId) from Person p"); // test ConditionInConstantSet

        checkQuery("select p.name, ? from Person p where name regexp ? and p.old < ?");

        checkQuery("select count(*) as a from Person");
        checkQuery("select count(*) as a, count(p.*), count(p.name) from Person p");
        checkQuery("select count(distinct p.name) from Person p");

        checkQuery("select p.name, avg(p.old), max(p.old) from Person p group by p.name");
        checkQuery("select p.name n, avg(p.old) a, max(p.old) m from Person p group by p.name");
        checkQuery("select p.name n, avg(p.old) a, max(p.old) m from Person p group by n");

        checkQuery("select p.name n, avg(p.old) a, max(p.old) m from Person p group by p.addrId, p.name");
        checkQuery("select p.name n, avg(p.old) a, max(p.old) m from Person p group by p.name, p.addrId");
        checkQuery("select p.name n, max(p.old) + min(p.old) / count(distinct p.old) from Person p group by p.name");
        checkQuery("select p.name n, max(p.old) maxOld, min(p.old) minOld from Person p group by p.name having maxOld > 10 and min(p.old) < 1");

        checkQuery("select p.name n, avg(p.old) a, max(p.old) m from Person p group by p.name order by n");
        checkQuery("select p.name n, avg(p.old) a, max(p.old) m from Person p group by p.name order by p.name");
        checkQuery("select p.name n, avg(p.old) a, max(p.old) m from Person p group by p.name order by p.name, m");
        checkQuery("select p.name n, avg(p.old) a, max(p.old) m from Person p group by p.name order by p.name, max(p.old) desc");
        checkQuery("select p.name n, avg(p.old) a, max(p.old) m from Person p group by p.name order by p.name nulls first");
        checkQuery("select p.name n, avg(p.old) a, max(p.old) m from Person p group by p.name order by p.name nulls last");
        checkQuery("select p.name n from Person p order by p.old + 10");
        checkQuery("select p.name n from Person p order by p.old + 10, p.name");
        checkQuery("select p.name n from Person p order by p.old + 10, p.name desc");

        checkQuery("select p.name n from Person p, (select a.street from Address a where a.street is not null) ");
        checkQuery("select street from Person p, (select a.street from Address a where a.street is not null) ");
        checkQuery("select addr.street from Person p, (select a.street from Address a where a.street is not null) addr");

        checkQuery("select p.name n from PUBLIC.Person p order by p.old + 10");
    }

    /**
     *
     */
    public void testExample1() throws Exception {
        Select select = parse("select p.name n, max(p.old) maxOld, min(p.old) minOld from Person p group by p.name having maxOld > 10 and min(p.old) < 1");

        GridQueryParser ses = new GridQueryParser();

        GridSelect gridSelect = ses.toGridSelect(select);

        //System.out.println(select.getPlanSQL());
        System.out.println(gridSelect.getSQL());
    }

    /**
     *
     */
    private JdbcConnection connection() throws Exception {
        GridKernalContext ctx = ((GridKernal)ignite).context();

        GridQueryProcessor qryProcessor = ctx.query();

        GridH2Indexing idx = (GridH2Indexing)GridQueryUtils.getFieldValue(GridQueryProcessor.class, qryProcessor,
            "idx");

        return (JdbcConnection)idx.connectionForSpace(null);
    }

    /**
     * @param sql Sql.
     */
    private GridSelect toGridSelect(String sql) throws Exception {
        Session ses = (Session)connection().getSession();

        Select select = (Select)ses.prepare(sql);

        return new GridQueryParser().toGridSelect(select);
    }

    /**
     * @param sql Sql.
     */
    private <T extends Prepared> T parse(String sql) throws Exception {
        Session ses = (Session)connection().getSession();

        return (T)ses.prepare(sql);
    }

    /**
     * @param sql1 Sql 1.
     * @param sql2 Sql 2.
     */
    private void assertSqlEquals(String sql1, String sql2) {
        assertEquals(normalizeSql(sql1), normalizeSql(sql2));
    }

    /**
     * @param sql Sql.
     */
    private static String normalizeSql(String sql) {
        return sql.toLowerCase()
            .replaceAll("/\\*(?:.|\r|\n)*?\\*/", " ")
            .replaceAll("\\s*on\\s+1\\s*=\\s*1\\s*", " ")
            .replaceAll("\\s+", " ")
            .replaceAll("\\( +", "(")
            .replaceAll(" +\\)", ")")
            .trim();
    }

    /**
     * @param qry Query.
     */
    private void checkQuery(String qry) throws Exception {
        Prepared prepared = parse(qry);

        GridQueryParser ses = new GridQueryParser();

        String res;

        if (prepared instanceof Select)
            res = ses.toGridSelect((Select)prepared).getSQL();
        else
            throw new UnsupportedOperationException();

        assertSqlEquals(prepared.getPlanSQL(), res);

        System.out.println(normalizeSql(res));
    }

    /**
     *
     */
    public static class Person implements Serializable {
        @GridCacheQuerySqlField(index = true)
        public Date date = new Date();

        @GridCacheQuerySqlField(index = true)
        public String name = "Ivan";

        @GridCacheQuerySqlField(index = true)
        public String parentName;

        @GridCacheQuerySqlField(index = true)
        public int addrId;

        @GridCacheQuerySqlField(index = true)
        public int old;
    }

    /**
     *
     */
    public static class Address implements Serializable {
        @GridCacheQuerySqlField(index = true)
        public int id;

        @GridCacheQuerySqlField(index = true)
        public int streetNumber;

        @GridCacheQuerySqlField(index = true)
        public String street = "Nevskiy";
    }

}

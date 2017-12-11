package org.apache.ignite.internal.processors.cache.index;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.internal.processors.cache.query.SqlFieldsQueryEx;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.testframework.junits.IgniteTestResources;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Verifies index naming for Optimized Marshaller case.
 *
 * See IGNITE-6915 for details.
 */

public class OptimizedMarshallerIndexNameTest extends GridCommonAbstractTest {

    private static final String TEST_NAME = "Naum Prikhodiaschiy";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName, IgniteTestResources rsrcs)
        throws Exception {

        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName, rsrcs);

        cfg.setMarshaller(new OptimizedMarshaller());

        return cfg;
    }

    protected static CacheConfiguration cacheConfiguration(String name) {
        CacheConfiguration cfg = new CacheConfiguration(name);

        cfg.setAtomicityMode(TRANSACTIONAL);
        cfg.setNearConfiguration(new NearCacheConfiguration());
        cfg.setWriteSynchronizationMode(FULL_SYNC);
        cfg.setEvictionPolicy(null);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid(getTestIgniteInstanceName());
    }

    public void testOptimizedMarshallerIndex() throws Exception {

        UUID uuid = UUID.randomUUID();

        // Cache 1
        CacheConfiguration ccfg1 = cacheConfiguration("PersonEn");

        ccfg1.setIndexedTypes(UUID.class, NamespaceEn.Person.class);

        IgniteCache<UUID, NamespaceEn.Person> cache1 = grid().getOrCreateCache(ccfg1);

        cache1.put(uuid, new NamespaceEn.Person(TEST_NAME, 42));

        // Cache 2
        CacheConfiguration ccfg2 = cacheConfiguration("PersonRu");

        ccfg2.setIndexedTypes(UUID.class, NamespaceRu.Person.class);

        IgniteCache<UUID, NamespaceRu.Person> cache2 = grid().getOrCreateCache(ccfg2);

        cache2.put(uuid, new NamespaceRu.Person(TEST_NAME, 42));

        // Check
        SqlFieldsQueryEx qry = new SqlFieldsQueryEx(
            "select * from " + QueryUtils.typeName(NamespaceEn.Person.class) + " where _key = ?", true);
        qry.setArgs(uuid);

        assertEquals(TEST_NAME, cache1.query(qry).getAll().get(0).get(0));

        qry = new SqlFieldsQueryEx(
            "select * from " + QueryUtils.typeName(NamespaceRu.Person.class) + " where _key = ?", true);
        qry.setArgs(uuid);

        assertEquals(TEST_NAME, cache2.query(qry).getAll().get(0).get(0));
    }

    public static class NamespaceEn {

        public static class Person implements Externalizable {

            @QuerySqlField(index = true)
            private String name;

            @QuerySqlField(index = true)
            private int age;

            public Person() {
            }

            public Person(String name, int age) {
                this.name = name;
                this.age = age;
            }

            public String getName() {
                return name;
            }

            public int getAge() {
                return age;
            }

            @Override public void writeExternal(ObjectOutput out) throws IOException {
                out.writeUTF(name);
                out.writeInt(age);
            }

            @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
                name = in.readUTF();
                age = in.readInt();
            }
        }
    }

    public static class NamespaceRu {

        public static class Person implements Externalizable {

            @QuerySqlField(index = true)
            private String name;

            @QuerySqlField(index = true)
            private int age;

            public Person() {
            }

            public Person(String name, int age) {
                this.name = name;
                this.age = age;
            }

            public String getName() {
                return name;
            }

            public int getAge() {
                return age;
            }

            @Override public void writeExternal(ObjectOutput out) throws IOException {
                out.writeUTF(name);
                out.writeInt(age);
            }

            @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
                name = in.readUTF();
                age = in.readInt();
            }
        }
    }
}

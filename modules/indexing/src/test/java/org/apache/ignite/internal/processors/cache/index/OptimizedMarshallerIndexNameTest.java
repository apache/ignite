package org.apache.ignite.internal.processors.cache.index;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.internal.processors.cache.query.SqlFieldsQueryEx;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.testframework.junits.IgniteTestResources;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Objects;
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
    private static final String TEST_NAME2 = "Mozgobaum";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName, IgniteTestResources rsrcs)
        throws Exception {

        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName, rsrcs);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setMaxSize(300 * 1024 * 1024).setPersistenceEnabled(true))
            .setWalMode(WALMode.LOG_ONLY);

        cfg.setDataStorageConfiguration(memCfg);

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
        grid().active(true);
    }

    public void testOptimizedMarshallerIndex() throws Exception {

        UUID uuid = UUID.randomUUID();

        // Cache 1
        CacheConfiguration ccfg1 = cacheConfiguration("PersonEn");

        ccfg1.setIndexedTypes(NamespaceEn.Key.class, NamespaceEn.Person.class);

        IgniteCache<NamespaceEn.Key, NamespaceEn.Person> cache1 = grid().getOrCreateCache(ccfg1);

        cache1.put(new NamespaceEn.Key(uuid), new NamespaceEn.Person(TEST_NAME, 42));

        // Cache 2
        CacheConfiguration ccfg2 = cacheConfiguration("PersonRu");

        ccfg2.setIndexedTypes(NamespaceRu.Key.class, NamespaceRu.Person.class);

        IgniteCache<NamespaceRu.Key, NamespaceRu.Person> cache2 = grid().getOrCreateCache(ccfg2);

        cache2.put(new NamespaceRu.Key(uuid), new NamespaceRu.Person(TEST_NAME2, 42));

        // Check
        SqlFieldsQueryEx qry = new SqlFieldsQueryEx(
            "select * from " + QueryUtils.typeName(NamespaceEn.Person.class) + " where _key = ?", true);
        qry.setArgs(new NamespaceEn.Key(uuid));

        assertEquals(TEST_NAME, cache1.query(qry).getAll().get(0).get(0));

        qry = new SqlFieldsQueryEx(
            "select * from " + QueryUtils.typeName(NamespaceRu.Person.class) + " where _key = ?", true);
        qry.setArgs(new NamespaceRu.Key(uuid));

        assertEquals(TEST_NAME, cache2.query(qry).getAll().get(0).get(0));
    }

    public static class NamespaceEn {

        public static class Key implements Externalizable {
            private UUID uuid;

            public UUID getUuid() {
                return uuid;
            }

            public Key() {
            }

            public Key(UUID uuid) {
                this.uuid = uuid;
            }

            @Override public void writeExternal(ObjectOutput out) throws IOException {
                out.writeUTF(uuid.toString());
            }

            @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
                uuid = UUID.fromString(in.readUTF());
            }

            @Override public boolean equals(Object o) {
                if (this == o)
                    return true;
                if (o == null || getClass() != o.getClass())
                    return false;
                Key key = (Key)o;
                return Objects.equals(uuid, key.uuid);
            }

            @Override public int hashCode() {
                return Objects.hash(uuid);
            }
        }

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

        public static class Key implements Externalizable {
            private UUID uuid;

            public UUID getUuid() {
                return uuid;
            }

            public Key() {
            }

            public Key(UUID uuid) {
                this.uuid = uuid;
            }

            @Override public void writeExternal(ObjectOutput out) throws IOException {
                out.writeUTF(uuid.toString());
            }

            @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
                uuid = UUID.fromString(in.readUTF());
            }

            @Override public boolean equals(Object o) {
                if (this == o)
                    return true;
                if (o == null || getClass() != o.getClass())
                    return false;
                NamespaceRu.Key key = (NamespaceRu.Key)o;
                return Objects.equals(uuid, key.uuid);
            }

            @Override public int hashCode() {
                return Objects.hash(uuid);
            }

        }

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

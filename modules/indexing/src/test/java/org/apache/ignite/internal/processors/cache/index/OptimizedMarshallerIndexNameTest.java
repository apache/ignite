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
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.IgniteTestResources;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
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

    private static final String TEST_NAME1 = "Name1";
    private static final String TEST_NAME2 = "Name2";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName, IgniteTestResources rsrcs)
        throws Exception {

        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName, rsrcs);

        // Bug IGNITE-6915 reproduces only when persistence is enabled and optimized marshaller is employed

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setMaxSize(300 * 1024 * 1024).setPersistenceEnabled(true))
            .setStoragePath(workSubdir() + "/db")
            .setWalArchivePath(workSubdir() + "/db/wal/archive")
            .setWalPath(workSubdir() + "/db/wal")
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

        cfg.setIndexedTypes(
            Key.class, Person.class,
            FalseKey.class, FalsePerson.class);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {

        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), workSubdir(), true));

        startGrid(getTestIgniteInstanceName());
        grid().active(true);
    }

    public void testOptimizedMarshallerIndex() throws Exception {

        // Cache 1
        CacheConfiguration ccfg = cacheConfiguration("PersonEn");

        IgniteCache<Object, Object> cache1 = grid().getOrCreateCache(ccfg);

        cache1.put(new Key(UUID.randomUUID()), new Person(TEST_NAME1, 42));
        cache1.put(new FalseKey(UUID.randomUUID()), new FalsePerson(32, TEST_NAME2));

        // Check
        SqlFieldsQueryEx qry = new SqlFieldsQueryEx(
            "select * from " + QueryUtils.typeName(FalsePerson.class), true);

        List<List<?>> result = cache1.query(qry).getAll();

        assertEquals(1, result.size());
    }

    @NotNull private String workSubdir() {
        return getClass().getSimpleName();
    }

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

        @QuerySqlField(index = true, inlineSize = 0)
        private String name;

        @QuerySqlField(index = true, inlineSize = 0)
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


    public static class FalseKey implements Externalizable {
        private UUID uuid;

        public UUID getUuid() {
            return uuid;
        }

        public FalseKey() {
        }

        public FalseKey(UUID uuid) {
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
            FalseKey key = (FalseKey)o;
            return Objects.equals(uuid, key.uuid);
        }

        @Override public int hashCode() {
            return Objects.hash(uuid);
        }

    }

    public static class FalsePerson implements Externalizable {

        @QuerySqlField(index = true, inlineSize = 0)
        private int name;

        @QuerySqlField(index = true, inlineSize = 0)
        private String age;

        public FalsePerson() {
        }

        public FalsePerson(int name, String age) {
            this.name = name;
            this.age = age;
        }

        public int getName() {
            return name;
        }

        public String getAge() {
            return age;
        }

        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(name);
            out.writeUTF(age);
        }

        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            name = in.readInt();
            age = in.readUTF();
        }
    }
}

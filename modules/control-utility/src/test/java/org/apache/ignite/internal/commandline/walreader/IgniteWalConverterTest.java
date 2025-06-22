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

package org.apache.ignite.internal.commandline.walreader;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.logger.IgniteLoggerEx;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.record.PageSnapshot;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordV1Serializer;
import org.apache.ignite.internal.processors.platform.cache.expiry.PlatformExpiryPolicyFactory;
import org.apache.ignite.internal.util.lang.IgniteThrowableConsumer;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.encryption.keystore.KeystoreEncryptionSpi;
import org.apache.ignite.spi.systemview.view.CacheView;
import org.apache.ignite.util.GridCommandHandlerAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runners.Parameterized;

import static java.util.Collections.emptyList;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_WAL_PATH;
import static org.apache.ignite.internal.encryption.AbstractEncryptionTest.KEYSTORE_PASSWORD;
import static org.apache.ignite.internal.encryption.AbstractEncryptionTest.KEYSTORE_PATH;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;

/**
 * Test for IgniteWalConverter
 */
public class IgniteWalConverterTest extends GridCommandHandlerAbstractTest {
    /** */
    public static final String PERSON_NAME_PREFIX = "Name ";

    /** */
    public static final long DURATION_AMOUNT = 200L;

    /** Flag "skip CRC calculation" in system property save before test and restore after. */
    private String beforeIgnitePdsSkipCrc;

    /** Flag "skip CRC calculation" in RecordV1Serializer save before test and restore after. */
    private boolean beforeSkipCrc;

    /** Encrypted. */
    public boolean encrypted;

    /** */
    @Parameterized.Parameters(name = "cmdHnd={0}")
    public static List<String> commandHandlers() {
        return F.asList(CLI_CMD_HND);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        beforeIgnitePdsSkipCrc = System.getProperty(IgniteSystemProperties.IGNITE_PDS_SKIP_CRC);
        beforeSkipCrc = RecordV1Serializer.skipCrc;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        encrypted = false;

        injectTestSystemOut();

        stopAllGrids();
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids(true);

        cleanPersistenceDir();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        if (beforeIgnitePdsSkipCrc != null)
            System.setProperty(IgniteSystemProperties.IGNITE_PDS_SKIP_CRC, beforeIgnitePdsSkipCrc);
        else
            System.clearProperty(IgniteSystemProperties.IGNITE_PDS_SKIP_CRC);

        RecordV1Serializer.skipCrc = beforeSkipCrc;

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        final IgniteConfiguration igniteConfiguration = super.getConfiguration(igniteInstanceName);

        igniteConfiguration.setDataStorageConfiguration(getDataStorageConfiguration());

        final CacheConfiguration cacheConfiguration = new CacheConfiguration<>()
            .setEncryptionEnabled(encrypted)
            .setName(DEFAULT_CACHE_NAME)
            .setCacheMode(CacheMode.PARTITIONED)
            .setBackups(0)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setIndexedTypes(PersonKey.class, Person.class);

        if (encrypted)
            igniteConfiguration.setEncryptionSpi(encryptionSpi());

        igniteConfiguration.setCacheConfiguration(cacheConfiguration);

        return igniteConfiguration;
    }

    /** @return KeystoreEncryptionSpi */
    public static KeystoreEncryptionSpi encryptionSpi() {
        KeystoreEncryptionSpi encSpi = new KeystoreEncryptionSpi();

        encSpi.setKeyStorePath(KEYSTORE_PATH);
        encSpi.setKeyStorePassword(KEYSTORE_PASSWORD.toCharArray());

        return encSpi;
    }

    /** @return DataStorageConfiguration. */
    private DataStorageConfiguration getDataStorageConfiguration() {
        return new DataStorageConfiguration()
            .setWalSegmentSize(4 * 1024 * 1024)
            .setWalMode(WALMode.LOG_ONLY)
            .setCheckpointFrequency(1000)
            .setWalCompactionEnabled(true)
            .setDefaultDataRegionConfiguration(getDataRegionConfiguration());
    }

    /** @return DataRegionConfiguration. */
    private DataRegionConfiguration getDataRegionConfiguration() {
        return new DataRegionConfiguration()
            .setPersistenceEnabled(true)
            .setMaxSize(100L * 1024 * 1024);
    }

    /** @return ExpiryPolicy. */
    private ExpiryPolicy getPolicy() {
        return new CreatedExpiryPolicy(new Duration(TimeUnit.SECONDS, 200L));
    }

    /**
     * Rund wal reader with given parameters
     *
     * @param list List of {@link Person Persons}.
     * @param hasExpiryPlc Has expiry policy.
     * @param procSensitiveData Process sensitive data {@link ProcessSensitiveData enum}.
     */
    private String runWalConverter(List<Person> list, Boolean hasExpiryPlc, ProcessSensitiveData procSensitiveData) throws Exception {
        testOut.reset();

        final NodeFileTree ft = createWal(list, null, hasExpiryPlc);

        IgniteLogger log = createTestLogger();

        try (IgniteWalConverter converter = new IgniteWalConverter(
            ft,
            DataStorageConfiguration.DFLT_PAGE_SIZE,
            false,
            new HashSet<>(),
            null,
            null,
            null,
            procSensitiveData,
            true,
            true,
            emptyList(),
            log
        )) {
            converter.convert();
        }

        if (log instanceof IgniteLoggerEx)
            ((IgniteLoggerEx)log).flush();

        return testOut.toString();
    }

    /**
     * Checking utility IgniteWalConverter
     * <ul>
     *     <li>Start node</li>
     *     <li>Create cache with
     *     <a href="https://apacheignite.readme.io/docs/indexes#section-registering-indexed-types">Registering Indexed Types</a></li>
     *     <li>Put several entity</li>
     *     <li>Stop node</li>
     *     <li>Read wal with specifying binaryMetadata</li>
     *     <li>Check that the output contains all DataRecord with previously added entities</li>
     * </ul>
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIgniteWalConverter() throws Exception {
        final List<Person> list = new LinkedList<>();

        final String result = runWalConverter(list, false, ProcessSensitiveData.SHOW);

        int idx = 0;

        for (Person person : list) {
            boolean find = false;

            idx = result.indexOf("DataRecord", idx);

            if (idx > 0) {
                idx = result.indexOf("PersonKey", idx + 10);

                if (idx > 0) {
                    idx = result.indexOf("id=" + person.getId(), idx + 9);

                    if (idx > 0) {
                        idx = result.indexOf("name=" + person.getName(), idx + 4);

                        find = idx > 0;
                    }
                }
            }

            assertTrue("DataRecord for Person(id=" + person.getId() + ") not found", find);
        }
    }

    /**
     * Checking utility IgniteWalConverter without binary_meta
     * <ul>
     *     <li>Start node</li>
     *     <li>Create cache with
     *     <a href="https://apacheignite.readme.io/docs/indexes#section-registering-indexed-types">Registering Indexed Types</a></li>
     *     <li>Put several entity</li>
     *     <li>Stop node</li>
     *     <li>Read wal with <b>out</b> specifying binaryMetadata</li>
     *     <li>Check that the output contains all DataRecord and in DataRecord not empty key and value</li>
     * </ul>
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIgniteWalConverterWithOutBinaryMeta() throws Exception {
        final List<Person> list = new LinkedList<>();

        NodeFileTree ft = createWal(list, null, false);

        U.delete(ft.binaryMeta());
        U.delete(ft.marshaller());

        final String result = runWalConverter(list, false, ProcessSensitiveData.SHOW);

        int idx = 0;

        for (Person person : list) {
            boolean find = false;

            idx = result.indexOf("DataRecord", idx);

            if (idx > 0) {
                idx = result.indexOf(" v = [", idx + 10);

                if (idx > 0) {
                    int start = idx + 6;

                    idx = result.indexOf(']', start);

                    if (idx > 0) {
                        final String val = result.substring(start, idx);

                        find = new String(Base64.getDecoder().decode(val)).contains(person.getName());
                    }
                }
            }

            assertTrue("DataRecord for Person(id=" + person.getId() + ") not found", find);
        }
    }

    /**
     * Checking utility IgniteWalConverter on broken WAL
     * <ul>
     *     <li>Start node</li>
     *     <li>Create cache with
     *     <a href="https://apacheignite.readme.io/docs/indexes#section-registering-indexed-types">Registering Indexed Types</a></li>
     *     <li>Put several entity</li>
     *     <li>Stop node</li>
     *     <li>Change byte in DataRecord value</li>
     *     <li>Read wal</li>
     *     <li>Check one error when reading WAL</li>
     * </ul>
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIgniteWalConverterWithBrokenWal() throws Exception {
        final List<Person> list = new LinkedList<>();

        final NodeFileTree ft = createWal(list, null, false);

        final File wal = ft.walSegment(0);

        try (RandomAccessFile raf = new RandomAccessFile(wal, "rw")) {
            raf.seek(RecordV1Serializer.HEADER_RECORD_SIZE); // HeaderRecord

            byte findByte[] = (PERSON_NAME_PREFIX + 0).getBytes();

            boolean find = false;

            while (!find) {
                int recordTypeIdx = raf.read();

                if (recordTypeIdx > 0) {
                    recordTypeIdx--;

                    raf.readLong();

                    final int fileOff = Integer.reverseBytes(raf.readInt());

                    final int len = Integer.reverseBytes(raf.readInt());

                    if (recordTypeIdx == WALRecord.RecordType.DATA_RECORD_V2.index()) {
                        int i = 0;

                        int b;

                        while (!find && (b = raf.read()) >= 0) {
                            if (findByte[i] == b) {
                                i++;

                                if (i == findByte.length)
                                    find = true;
                            }
                            else
                                i = 0;
                        }
                        if (find) {
                            raf.seek(raf.getFilePointer() - 1);

                            raf.write(' ');
                        }
                    }

                    raf.seek(fileOff + len);
                }
            }
        }

        final String result = runWalConverter(list, false, ProcessSensitiveData.SHOW);

        int idx = 0;

        int cntErrorRead = 0;

        for (Person person : list) {
            boolean find = false;

            idx = result.indexOf("DataRecord", idx);

            if (idx > 0) {
                idx = result.indexOf("PersonKey", idx + 10);

                if (idx > 0) {
                    idx = result.indexOf("id=" + person.getId(), idx + 9);

                    if (idx > 0) {
                        idx = result.indexOf("name=" + person.getName(), idx + 4);

                        find = idx > 0;
                    }
                }
            }

            if (!find)
                cntErrorRead++;
        }
        assertEquals(1, cntErrorRead);
    }

    /**
     * Checking utility IgniteWalConverter on unreadable WAL
     * <ul>
     *     <li>Start node</li>
     *     <li>Create cache with
     *     <a href="https://apacheignite.readme.io/docs/indexes#section-registering-indexed-types">Registering Indexed Types</a></li>
     *     <li>Put several entity</li>
     *     <li>Stop node</li>
     *     <li>Change byte RecordType in second DataRecord</li>
     *     <li>Read wal</li>
     *     <li>Check contains one DataRecord in output before error when reading WAL</li>
     * </ul>
     *
     * @throws Exception If failed.
     */
//    @Test
    public void testIgniteWalConverterWithUnreadableWal() throws Exception {
        final List<Person> list = new LinkedList<>();

        final NodeFileTree ft = createWal(list, null, false);

        final File wal = ft.walSegment(0);

        try (RandomAccessFile raf = new RandomAccessFile(wal, "rw")) {
            raf.seek(RecordV1Serializer.HEADER_RECORD_SIZE); // HeaderRecord

            int find = 0;

            while (find < 2) {
                int recordTypeIdx = raf.read();

                if (recordTypeIdx > 0) {
                    recordTypeIdx--;

                    if (recordTypeIdx == WALRecord.RecordType.DATA_RECORD_V2.index()) {
                        find++;

                        if (find == 2) {
                            raf.seek(raf.getFilePointer() - 1);

                            raf.write(Byte.MAX_VALUE);
                        }
                    }
                    final int fileOff = Integer.reverseBytes(raf.readInt());

                    final int len = Integer.reverseBytes(raf.readInt());

                    raf.seek(fileOff + len);
                }
            }
        }

        final String result = runWalConverter(list, false, ProcessSensitiveData.SHOW);

        int idx = 0;

        int cntErrorRead = 0;

        for (Person person : list) {
            boolean find = false;

            idx = result.indexOf("DataRecord", idx);

            if (idx > 0) {
                idx = result.indexOf("PersonKey", idx + 10);

                if (idx > 0) {
                    idx = result.indexOf("id=" + person.getId(), idx + 9);

                    if (idx > 0) {
                        idx = result.indexOf(person.getClass().getSimpleName(), idx + 4);

                        if (idx > 0) {
                            idx = result.indexOf("id=" + person.getId(), idx + person.getClass().getSimpleName().length());

                            if (idx > 0) {
                                idx = result.indexOf("name=" + person.getName(), idx + 4);

                                find = idx > 0;
                            }
                        }
                    }
                }
            }

            if (!find)
                cntErrorRead++;
        }

        assertEquals(9, cntErrorRead);
    }

    /**
     * Check that when using the "pages" argument we will see WalRecord with these pages in the utility output.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPages() throws Exception {
        testOut.reset();

        List<T2<PageSnapshot, String>> walRecords = new ArrayList<>();

        NodeFileTree ft = createWal(new ArrayList<>(), n -> {
            try (WALIterator walIter = n.context().cache().context().wal().replay(new WALPointer(0, 0, 0))) {
                while (walIter.hasNextX()) {
                    WALRecord walRecord = walIter.nextX().get2();

                    if (walRecord instanceof PageSnapshot)
                        walRecords.add(new T2<>((PageSnapshot)walRecord, walRecord.toString()));
                }
            }
        },
            false);

        assertFalse(walRecords.isEmpty());

        File walDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_WAL_PATH, false);

        assertTrue(U.fileCount(walDir.toPath()) > 0);

        assertTrue(U.fileCount(ft.wal().toPath()) > 0);

        T2<PageSnapshot, String> expRec = walRecords.get(0);

        Collection<T2<Integer, Long>> pages =
                IgniteWalConverter.collectPages(
                        "" + expRec.get1().fullPageId().groupId() + ':' + expRec.get1().fullPageId().pageId());

        try (IgniteWalConverter converter = new IgniteWalConverter(
                ft,
                DataStorageConfiguration.DFLT_PAGE_SIZE,
                false,
                new HashSet<>(),
                null,
                null,
                null,
                ProcessSensitiveData.SHOW,
                true,
                true,
                pages,
                log
        )) {
            converter.convert();
        }

//        assertContains(log, testOut.toString(), expRec.get2());
        assertTrue(testOut.toString().contains(expRec.get2()));
    }

    /**
     * Test for {@link CacheView} expiry policy factory representation. The test initializes the {@link CacheConfiguration}
     * with custom {@link PlatformExpiryPolicyFactory}. Given different ttl input, the test checks the {@link String}
     * expiry policy factory outcome for {@link CacheView#expiryPolicyFactory()}.
     */
    @Test
    public void testCacheViewExpiryPolicy() throws Exception {
        String result = runWalConverter(new LinkedList<>(), true, ProcessSensitiveData.SHOW);

        assertTrue(result.contains("expireTime="));
    }

    /**
     * Tests processing sensitive data flag
     */
    @Test
    public void testHideSensitiveData() throws Exception {
        final List<Person> list = new LinkedList<>();

        String result = runWalConverter(list, false, ProcessSensitiveData.HIDE);

        int idx;

        for (Person ignored : list) {
            boolean find;

            idx = result.indexOf("DataRecord");

            if (idx > 0) {
                idx = result.indexOf("UnwrapDataEntry", idx + "DataRecord".length());

                if (idx > 0)
                    idx = result.indexOf("k = , v = ", idx + "UnwrapDataEntry".length());
            }

            find = idx == -1;

            assertFalse(find);
        }
    }

    /**
     * Common part
     * <ul>
     *    <li>Start node</li>
     *    <li>Create cache with
     *    <a href="https://apacheignite.readme.io/docs/indexes#section-registering-indexed-types">Registering Indexed Types</a></li>
     *    <li>Put several entity</li>
     * </ul>
     *
     * @param list Returns entities that have been added.
     * @param afterPopulateConsumer
     * @return Ignite directories structure.
     *  @throws Exception
     */
    private NodeFileTree createWal(
        List<Person> list,
        @Nullable IgniteThrowableConsumer<IgniteEx> afterPopulateConsumer,
        @Nullable Boolean hasExpiryPlc
    ) throws Exception {
        NodeFileTree ft;

        try (final IgniteEx node = startGrid(0)) {
            node.cluster().state(ClusterState.ACTIVE);

            ft = node.context().pdsFolderResolver().fileTree();

            final IgniteCache<PersonKey, Person> cache = node.cache(DEFAULT_CACHE_NAME);

            for (int i = 0; i < 10; i++) {
                final PersonKey key = new PersonKey(i);

                final Person val;

                if (i % 2 == 0)
                    val = new Person(i, PERSON_NAME_PREFIX + i);
                else
                    val = new PersonEx(i, PERSON_NAME_PREFIX + i, "Additional information " + i, "Description " + i);

                if (hasExpiryPlc) {
                    ExpiryPolicy expiryPlc = new CreatedExpiryPolicy(new Duration(TimeUnit.SECONDS, DURATION_AMOUNT));
                    cache.withExpiryPolicy(expiryPlc).put(key, val);
                }
                else
                    cache.put(key, val);

                list.add(val);
            }

            if (afterPopulateConsumer != null)
                afterPopulateConsumer.accept(node);
        }

        return ft;
    }


    /**
     * Populates an encrypted cache and checks that its WAL contains encrypted records.
     */
    @Test
    public void testEncryptedIgniteWalConverter() throws Exception {
        encrypted = true;

        String result = runWalConverter(new LinkedList<>(), false, ProcessSensitiveData.SHOW);

        assertContains(log, result, "EncryptedRecord");
    }

    /**
     * A person entity used for the tests.
     */
    public static class Person {
        /** Id. */
        private final Integer id;

        /** Name. */
        @QuerySqlField
        private final String name;

        /** Constructor. */
        public Person(Integer id, String name) {
            this.id = id;
            this.name = name;
        }

        /** @return id. */
        public Integer getId() {
            return id;
        }

        /** @return name. */
        public String getName() {
            return name;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(id, name);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            if (!(obj instanceof Person))
                return false;

            Person other = (Person)obj;

            return Objects.equals(id, other.id) &&
                Objects.equals(name, other.name);
        }
    }

    /**
     * A person entity used for the tests.
     */
    public static class PersonEx extends Person {
        /**
         * Additional information.
         */
        @QuerySqlField
        private final String info;

        /**
         * Description - not declared as SQL field.
         */
        private final String description;

        /**
         * Constructor.
         */
        public PersonEx(Integer id, String name, String info, String description) {
            super(id, name);
            this.info = info;
            this.description = description;
        }

        /**
         * @return Additional information.
         */
        public String getInfo() {
            return info;
        }

        /**
         * @return Description.
         */
        public String getDescription() {
            return description;
        }

        /**
         * {@inheritDoc}
         */
        @Override public int hashCode() {
            return Objects.hash(super.hashCode(), info, description);
        }

        /**
         * {@inheritDoc}
         */
        @Override public boolean equals(Object obj) {
            if (!(obj instanceof PersonEx))
                return false;

            PersonEx other = (PersonEx)obj;

            return super.equals(other) && Objects.equals(info, other.info) && Objects.equals(description, other.description);
        }
    }

    /**
     * A person entity used for the tests.
     */
    public static class PersonKey {
        /**
         * Id.
         */
        @QuerySqlField(index = true)
        private final Integer id;

        /**
         * Constructor.
         */
        public PersonKey(Integer id) {
            this.id = id;
        }

        /**
         * @return id.
         */
        public Integer getId() {
            return id;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            if (!(obj instanceof PersonKey))
                return false;

            PersonKey other = (PersonKey)obj;

            return Objects.equals(other.id, id);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(id);
        }
    }
}

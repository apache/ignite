/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache;

import javax.cache.configuration.Factory;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.expiry.Duration;
import javax.cache.expiry.EternalExpiryPolicy;
import javax.cache.expiry.ExpiryPolicy;
import java.io.Serializable;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicyFactory;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Abstract data types coverage  test.
 */
@RunWith(Parameterized.class)
public abstract class AbstractDataTypesCoverageTest extends GridCommonAbstractTest {
    /** */
    @SuppressWarnings("unchecked")
    private static final Factory[] TTL_FACTORIES = {
        null,
        new FactoryBuilder.SingletonFactory<ExpiryPolicy>(new EternalExpiryPolicy()) {
            @Override public String toString() {
                return "EternalExpiryPolicy";
            }
        },
        new FactoryBuilder.SingletonFactory(new TestPolicy(60_000L, 61_000L, 62_000L)) {
            @Override public String toString() {
                return "ExpiryPolicy:60_000L, 61_000L, 62_000L";
            }
        }};

    /** */
    private static final Factory[] EVICTION_FACTORIES = {
        null,
        new FifoEvictionPolicyFactory(10, 1, 0) {
            @Override public String toString() {
                return "FifoEvictionPolicyFactory";
            }
        },
        // TODO: 28.08.19 https://ggsystems.atlassian.net/browse/GG-23429
//        new SortedEvictionPolicyFactory(10, 1, 0) {
//            @Override public String toString() {
//                return "SortedEvictionPolicyFactory";
//            }
//        }
    };

    /** Possible options for enabled/disabled properties like onheapCacheEnabled */
    private static final boolean[] BOOLEANS = {true, false};

    /** */
    protected static final int NODES_CNT = 3;

    /** */
    protected static final int TIMEOUT_FOR_KEY_RETRIEVAL_IN_FULL_ASYNC_MODE = 4_000;

    /** */
    protected static final int PARTITIONS_CNT = 16;

    /**
     * Here's an id of specific set of params is stored. It's used to overcome the limitations of junit4.11 in order to
     * pseudo-run @Before method only once after applying specific set of parameters. See {@code init()} for more
     * details.
     */
    private static UUID prevParamLineId;

    /** */
    @Parameterized.Parameter
    public UUID paramLineId;

    /** */
    @Parameterized.Parameter(1)
    public CacheAtomicityMode atomicityMode;

    /** */
    @Parameterized.Parameter(2)
    public CacheMode cacheMode;

    /** */
    @Parameterized.Parameter(3)
    public Factory<? extends ExpiryPolicy> ttlFactory;

    /** */
    @Parameterized.Parameter(4)
    public int backups;

    /** */
    @Parameterized.Parameter(5)
    public Factory evictionFactory;

    /** */
    @Parameterized.Parameter(6)
    public boolean onheapCacheEnabled;

    /** */
    @Parameterized.Parameter(7)
    public CacheWriteSynchronizationMode writeSyncMode;

    /** */
    @Parameterized.Parameter(8)
    public boolean persistenceEnabled;

    /**
     * @return Test parameters.
     */
    @Parameterized.Parameters(name = "atomicityMode={1}, cacheMode={2}, ttlFactory={3}, backups={4}," +
        " evictionFactory={5}, onheapCacheEnabled={6}, writeSyncMode={7}, persistenceEnabled={8}")
    public static Collection parameters() {
        Set<Object[]> params = new HashSet<>();

        Object[] baseParamLine = {
            null, CacheAtomicityMode.ATOMIC, CacheMode.PARTITIONED, null, 2, null,
            false, CacheWriteSynchronizationMode.FULL_SYNC, false};

        Object[] paramLine = null;

        for (CacheAtomicityMode atomicityMode : CacheAtomicityMode.values()) {
            paramLine = Arrays.copyOf(baseParamLine, baseParamLine.length);

            paramLine[1] = atomicityMode;

            params.add(paramLine);
        }

        for (CacheMode cacheMode : CacheMode.values()) {
            paramLine = Arrays.copyOf(baseParamLine, baseParamLine.length);

            paramLine[2] = cacheMode;

            params.add(paramLine);
        }

        assert paramLine != null;

        if ((paramLine[1]) != CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT) {
            for (Factory ttlFactory : TTL_FACTORIES) {
                paramLine = Arrays.copyOf(baseParamLine, baseParamLine.length);

                paramLine[3] = ttlFactory;

                params.add(paramLine);
            }
        }

        for (int backups : new int[] {0, 1, 2}) {
            paramLine = Arrays.copyOf(baseParamLine, baseParamLine.length);

            paramLine[4] = backups;

            params.add(paramLine);
        }

        for (Factory evictionFactory : EVICTION_FACTORIES) {
            paramLine = Arrays.copyOf(baseParamLine, baseParamLine.length);

            paramLine[5] = evictionFactory;

            params.add(paramLine);
        }

        for (Boolean onheapCacheEnabled : BOOLEANS) {
            paramLine = Arrays.copyOf(baseParamLine, baseParamLine.length);

            paramLine[6] = onheapCacheEnabled;

            params.add(paramLine);
        }

        for (CacheWriteSynchronizationMode writeSyncMode : CacheWriteSynchronizationMode.values()) {
            paramLine = Arrays.copyOf(baseParamLine, baseParamLine.length);

            paramLine[7] = writeSyncMode;

            params.add(paramLine);
        }

        for (boolean persistenceEnabled : BOOLEANS) {
            paramLine = Arrays.copyOf(baseParamLine, baseParamLine.length);

            paramLine[8] = persistenceEnabled;

            params.add(paramLine);
        }

        for (Object[] pLine : params)
            pLine[0] = UUID.randomUUID();

        return params;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 30_000;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration storageCfg = new DataStorageConfiguration();

        storageCfg.getDefaultDataRegionConfiguration().setMaxSize(200 * 1024 * 1024).
            setPersistenceEnabled(persistenceEnabled);

        cfg.setDataStorageConfiguration(storageCfg);

        return cfg;
    }

    /**
     * For any new set of parameters prepare new env:
     * <ul>
     * <li>stop all grids;</li>
     * <li>clean persistence dir;</li>
     * <li>start grids;</li>
     * <li>Update {@code prevParamLineId} with new value in order to process {@code init()} only once for specific set
     * of parameters but not once for every test method. That should be refactored after migration to junit 5 or junit
     * 4.13</li>
     * </ul>
     *
     * @throws Exception If failed.
     */
    public void init() throws Exception {
        if (!paramLineId.equals(prevParamLineId)) {
            stopAllGrids();

            cleanPersistenceDir();

            startGridsMultiThreaded(NODES_CNT);

            prevParamLineId = paramLineId;
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /**
     *
     */
    private static class TestPolicy implements ExpiryPolicy, Serializable {
        /** */
        private Long create;

        /** */
        private Long access;

        /** */
        private Long update;

        /**
         * @param create TTL for creation.
         * @param access TTL for access.
         * @param update TTL for update.
         */
        TestPolicy(@Nullable Long create,
            @Nullable Long update,
            @Nullable Long access) {
            this.create = create;
            this.update = update;
            this.access = access;
        }

        /** {@inheritDoc} */
        @Override public Duration getExpiryForCreation() {
            return create != null ? new Duration(TimeUnit.MILLISECONDS, create) : null;
        }

        /** {@inheritDoc} */
        @Override public Duration getExpiryForAccess() {
            return access != null ? new Duration(TimeUnit.MILLISECONDS, access) : null;
        }

        /** {@inheritDoc} */
        @Override public Duration getExpiryForUpdate() {
            return update != null ? new Duration(TimeUnit.MILLISECONDS, update) : null;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(TestPolicy.class, this);
        }
    }

    /**
     * Objects based on primitives only.
     */
    @SuppressWarnings("unused")
    protected static class ObjectBasedOnPrimitives implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private int intField;

        /** */
        private double doubleField;

        /** */
        private boolean booleanField;

        /**
         * @param intField Int field.
         * @param doubleField Double field.
         * @param booleanField Boolean field.
         */
        protected ObjectBasedOnPrimitives(int intField, double doubleField, boolean booleanField) {
            this.intField = intField;
            this.doubleField = doubleField;
            this.booleanField = booleanField;
        }

        /**
         * @return Int field.
         */
        public int intField() {
            return intField;
        }

        /**
         * @param intField New int field.
         */
        public void intField(int intField) {
            this.intField = intField;
        }

        /**
         * @return Double field.
         */
        public double doubleField() {
            return doubleField;
        }

        /**
         * @param doubleField New double field.
         */
        public void doubleField(double doubleField) {
            this.doubleField = doubleField;
        }

        /**
         * @return Boolean field.
         */
        public boolean booleanField() {
            return booleanField;
        }

        /**
         * @param booleanField New boolean field.
         */
        public void booleanField(boolean booleanField) {
            this.booleanField = booleanField;
        }
    }

    /**
     * Objects based on primitives and collections.
     */
    @SuppressWarnings({"unused", "AssignmentOrReturnOfFieldWithMutableType"})
    protected static class ObjectBasedOnPrimitivesAndCollections implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private int intField;

        /** */
        private List<Double> doubleListField;

        /** */
        private boolean[] booleanArrField;

        /**
         * @param intField Int field.
         * @param doubleListField Double list field.
         * @param booleanArrField Boolean array field.
         */
        protected ObjectBasedOnPrimitivesAndCollections(int intField, List<Double> doubleListField,
            boolean[] booleanArrField) {
            this.intField = intField;
            this.doubleListField = doubleListField;
            this.booleanArrField = booleanArrField;
        }

        /**
         * @return Int field.
         */
        public int intField() {
            return intField;
        }

        /**
         * @param intField New int field.
         */
        public void intField(int intField) {
            this.intField = intField;
        }

        /**
         * @return Double list field.
         */
        public List<Double> doubleListField() {
            return doubleListField;
        }

        /**
         * @param doubleListField New double list field.
         */
        public void doubleListField(List<Double> doubleListField) {
            this.doubleListField = doubleListField;
        }

        /**
         * @return Boolean array field.
         */
        public boolean[] booleanArrayField() {
            return booleanArrField;
        }

        /**
         * @param booleanArrField New boolean array field.
         */
        public void booleanArrayField(boolean[] booleanArrField) {
            this.booleanArrField = booleanArrField;
        }
    }

    /**
     * Objects based on primitives collections and nested objects.
     */
    @SuppressWarnings({"unused", "AssignmentOrReturnOfFieldWithMutableType"})
    protected static class ObjectBasedOnPrimitivesCollectionsAndNestedObject implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private int intField;

        /** */
        private List<Double> doubleListField;

        /** */
        private ObjectBasedOnPrimitivesAndCollections nestedObjField;

        /**
         * @param intField Int field.
         * @param doubleListField Double list field.
         * @param nestedObjField Nested object field.
         */
        protected ObjectBasedOnPrimitivesCollectionsAndNestedObject(int intField, List<Double> doubleListField,
            ObjectBasedOnPrimitivesAndCollections nestedObjField) {
            this.intField = intField;
            this.doubleListField = doubleListField;
            this.nestedObjField = nestedObjField;
        }

        /**
         * @return Int field.
         */
        public int intField() {
            return intField;
        }

        /**
         * @param intField New int field.
         */
        public void intField(int intField) {
            this.intField = intField;
        }

        /**
         * @return Double list field.
         */
        public List<Double> doubleListField() {
            return doubleListField;
        }

        /**
         * @param doubleListField New double list field.
         */
        public void doubleListField(List<Double> doubleListField) {
            this.doubleListField = doubleListField;
        }

        /**
         * @return Nested object field.
         */
        public ObjectBasedOnPrimitivesAndCollections nestedObjectField() {
            return nestedObjField;
        }

        /**
         * @param nestedObjField New nested object field.
         */
        public void nestedObjectField(ObjectBasedOnPrimitivesAndCollections nestedObjField) {
            this.nestedObjField = nestedObjField;
        }
    }

    /**
     * Holder for both original value and value to be used as part of sql string.
     */
    public interface SqlStrConvertedValHolder extends Serializable {
        /**
         * @return Original value that might be used as expected result.
         */
        Object originalVal();

        /**
         * @return Value converted to sql string representation.
         */
        String sqlStrVal();
    }

    /**
     * Holder for quoted values. E.g. Double.NEGATIVE_INFINITY -> 'INFINITY'.
     */
    public static class Quoted implements SqlStrConvertedValHolder {
        /** */
        private static final long serialVersionUID = 0L;

        /** Original value. */
        private Object val;

        /** Converted value. */
        private String sqlStrVal;

        /**
         * Constructor.
         *
         * @param val Original value.
         */
        public Quoted(Object val) {
            this.val = val;
            sqlStrVal = "\'" + val + "\'";
        }

        /** @inheritDoc */
        @Override public Object originalVal() {
            return val;
        }

        /** @inheritDoc */
        @Override public String sqlStrVal() {
            return sqlStrVal;
        }
    }

    /**
     * Holder for Sql Time values.
     */
    public static class Timed implements SqlStrConvertedValHolder {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private static final String PATTERN = "HH:mm:ss.SSS";

        /** */
        private static final SimpleDateFormat TIME_DATE_FORMAT = new SimpleDateFormat(PATTERN);

        /** Original value. */
        private Object val;

        /** Converted value. */
        private String sqlStrVal;

        /**
         * Constructor.
         *
         * @param time Original value.
         */
        public Timed(Time time) {
            val = time;
            sqlStrVal = "PARSEDATETIME('" + TIME_DATE_FORMAT.format(time) + "', '" + PATTERN + "')";
        }

        /**
         * Constructor.
         *
         * @param time Original local time value.
         */
        public Timed(LocalTime time) {
            val = time;
            sqlStrVal = "PARSEDATETIME('" + TIME_DATE_FORMAT.format(
                java.sql.Time.valueOf(time)) + "', '" + PATTERN + "')";
        }

        /** @inheritDoc */
        @Override public Object originalVal() {
            return val;
        }

        /** @inheritDoc */
        @Override public String sqlStrVal() {
            return sqlStrVal;
        }
    }

    /**
     * Holder for Date and Timestamp values.
     */
    public static class Dated implements SqlStrConvertedValHolder {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private static final String PATTERN = "yyyy-MM-dd HH:mm:ss.SSS";

        /** */
        private static final SimpleDateFormat DATE_TIME_DATE_FORMAT = new SimpleDateFormat(PATTERN);

        /** Original value. */
        private Object val;

        /** Converted value. */
        private String sqlStrVal;

        /**
         * Constructor.
         *
         * @param date Original value.
         */
        public Dated(Date date) {
            val = date;
            sqlStrVal = "PARSEDATETIME('" + DATE_TIME_DATE_FORMAT.format(date) + "', '" + PATTERN + "')";
        }

        /**
         * Constructor.
         *
         * @param date Original LocalDateTime value.
         */
        public Dated(LocalDateTime date) {
            val = date;
            sqlStrVal = "PARSEDATETIME('" + DATE_TIME_DATE_FORMAT.format(
                java.sql.Timestamp.valueOf(date)) + "', '" + PATTERN + "')";
        }

        /**
         * Constructor.
         *
         * @param ts Original Timestamp value.
         */
        public Dated(Timestamp ts, String ptrn) {
            val = ts;
            sqlStrVal = "PARSEDATETIME('" + DATE_TIME_DATE_FORMAT.format(ts) + "', '" + ptrn + "')";
        }

        /**
         * Constructor.
         *
         * @param date Original LocalDate value.
         */
        public Dated(LocalDate date) {
            val = date;
            sqlStrVal = "PARSEDATETIME('" + DATE_TIME_DATE_FORMAT.format(
                java.sql.Timestamp.valueOf(date.atStartOfDay())) + "', '" + PATTERN + "')";
        }

        /** @inheritDoc */
        @Override public Object originalVal() {
            return val;
        }

        /** @inheritDoc */
        @Override public String sqlStrVal() {
            return sqlStrVal;
        }
    }

    /**
     * Holder for byte array values.
     */
    public static class ByteArrayed implements SqlStrConvertedValHolder {
        /** */
        private static final long serialVersionUID = 0L;

        /** Original value. */
        private Object val;

        /** Converted value. */
        private String sqlStrVal;

        /**
         * Constructor.
         *
         * @param byteArr Original value.
         */
        public ByteArrayed(byte[] byteArr) {
            val = byteArr;
            sqlStrVal = "x'" + U.byteArray2HexString(byteArr) + "'";
        }

        /** @inheritDoc */
        @Override public Object originalVal() {
            return val;
        }

        /** @inheritDoc */
        @Override public String sqlStrVal() {
            return sqlStrVal;
        }
    }
}
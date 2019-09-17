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

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Data types coverage for basic cache operations.
 */
@RunWith(Parameterized.class)
@SuppressWarnings("ZeroLengthArrayAllocation")
public class GridCacheDataTypesCoverageTest extends AbstractDataTypesCoverageTest {

    /** @inheritDoc */
    @Before
    @Override public void init() throws Exception {
        super.init();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testByteDataType() throws Exception {
        checkBasicCacheOperations(
            Byte.MIN_VALUE,
            Byte.MAX_VALUE,
            (byte)0,
            (byte)1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testShortDataType() throws Exception {
        checkBasicCacheOperations(
            Short.MIN_VALUE,
            Short.MAX_VALUE,
            (short)0,
            (short)1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testIntegerDataType() throws Exception {
        checkBasicCacheOperations(
            Integer.MIN_VALUE,
            Integer.MAX_VALUE,
            0,
            1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLongDataType() throws Exception {
        checkBasicCacheOperations(
            Long.MIN_VALUE,
            Long.MAX_VALUE,
            0L,
            1L);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFloatDataType() throws Exception {
        checkBasicCacheOperations(
            Float.MIN_VALUE,
            Float.MAX_VALUE,
            Float.NaN,
            Float.NEGATIVE_INFINITY,
            Float.POSITIVE_INFINITY,
            0F,
            0.0F,
            1F,
            1.1F);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDoubleDataType() throws Exception {
        checkBasicCacheOperations(
            Double.MIN_VALUE,
            Double.MAX_VALUE,
            Double.NaN,
            Double.NEGATIVE_INFINITY,
            Double.POSITIVE_INFINITY,
            0D,
            0.0D,
            1D,
            1.1D);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBooleanDataType() throws Exception {
        checkBasicCacheOperations(
            Boolean.TRUE,
            Boolean.FALSE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCharacterDataType() throws Exception {
        checkBasicCacheOperations(
            'a',
            'A');
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStringDataType() throws Exception {
        checkBasicCacheOperations(
            "aAbB",
            "");
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://ggsystems.atlassian.net/browse/GG-23434")
    @Test
    public void testByteArrayDataType() throws Exception {
        checkBasicCacheOperations(
            new Byte[] {},
            new Byte[] {1, 2, 3},
            new byte[] {3, 2, 1});
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testObjectArrayDataType() throws Exception {
        checkBasicCacheOperations(
            new Object[] {},
            new Object[] {"String", Boolean.TRUE, 'A', 1},
            new Object[] {
                "String",
                new ObjectBasedOnPrimitives(123, 123.123, true)});
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testListDataType() throws Exception {
        checkBasicCacheOperations(
            new ArrayList<>(),
            (Serializable)Collections.singletonList("Aaa"),
            (Serializable)Arrays.asList("String", Boolean.TRUE, 'A', 1));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSetDataType() throws Exception {
        checkBasicCacheOperations(
            new HashSet<>(),
            (Serializable)Collections.singleton("Aaa"),
            new HashSet<>(Arrays.asList("String", Boolean.TRUE, 'A', 1)));
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://ggsystems.atlassian.net/browse/GG-23435")
    @Test
    public void testQueueDataType() throws Exception {
        ArrayBlockingQueue<Integer> queueToCheck = new ArrayBlockingQueue<>(5);
        queueToCheck.addAll(Arrays.asList(1, 2, 3));

        checkBasicCacheOperations(
            new LinkedList<>(),
            new LinkedList<>(Arrays.asList("Aaa", "Bbb")),
            queueToCheck);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testObjectBasedOnPrimitivesDataType() throws Exception {
        checkBasicCacheOperations(
            new ObjectBasedOnPrimitives(0, 0.0, false),
            new ObjectBasedOnPrimitives(123, 123.123, true));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testObjectBasedOnPrimitivesAndCollectionsDataType() throws Exception {
        checkBasicCacheOperations(
            new ObjectBasedOnPrimitivesAndCollections(0,
                Collections.emptyList(),
                new boolean[] {}),
            new ObjectBasedOnPrimitivesAndCollections(123,
                Arrays.asList(1.0, 0.0),
                new boolean[] {true, false}));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testObjectBasedOnPrimitivesAndCollectionsAndNestedObjectsDataType() throws Exception {
        checkBasicCacheOperations(
            new ObjectBasedOnPrimitivesCollectionsAndNestedObject(0,
                Collections.emptyList(),
                new ObjectBasedOnPrimitivesAndCollections(
                    0,
                    Collections.emptyList(),
                    new boolean[] {})),
            new ObjectBasedOnPrimitivesCollectionsAndNestedObject(-1,
                Collections.singletonList(0.0),
                new ObjectBasedOnPrimitivesAndCollections(
                    Integer.MAX_VALUE,
                    new ArrayList<>(Collections.singleton(22.2)),
                    new boolean[] {true, false, true})));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDateDataType() throws Exception {
        checkBasicCacheOperations(
            new Date(),
            new Date(Long.MIN_VALUE),
            new Date(Long.MAX_VALUE));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSqlDateDataType() throws Exception {
        checkBasicCacheOperations(
            new java.sql.Date(Long.MIN_VALUE),
            new java.sql.Date(Long.MAX_VALUE));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCalendarDataType() throws Exception {
        checkBasicCacheOperations(new GregorianCalendar());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInstantDataType() throws Exception {
        checkBasicCacheOperations(
            Instant.now(),
            Instant.ofEpochMilli(Long.MIN_VALUE),
            Instant.ofEpochMilli(Long.MAX_VALUE));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLocalDateDataType() throws Exception {
        checkBasicCacheOperations(
            LocalDate.of(2015, 2, 20),
            LocalDate.now().plusDays(1));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLocalDateTimeDataType() throws Exception {
        checkBasicCacheOperations(
            LocalDateTime.of(2015, 2, 20, 9, 4, 30),
            LocalDateTime.now().plusDays(1));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLocalTimeDataType() throws Exception {
        checkBasicCacheOperations(
            LocalTime.of(9, 4, 40),
            LocalTime.now());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBigIntegerDataType() throws Exception {
        checkBasicCacheOperations(
            new BigInteger("1"),
            BigInteger.ONE,
            BigInteger.ZERO,
            new BigInteger("123456789"));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBigDecimalDataType() throws Exception {
        checkBasicCacheOperations(
            new BigDecimal(123.123),
            BigDecimal.ONE,
            BigDecimal.ZERO,
            BigDecimal.valueOf(123456789, 0),
            BigDecimal.valueOf(123456789, 1),
            BigDecimal.valueOf(123456789, 2),
            BigDecimal.valueOf(123456789, 3));
    }

    /**
     * Verification that following cache methods works correctly:
     *
     * <ul>
     * <li>put</li>
     * <li>putAll</li>
     * <li>remove</li>
     * <li>removeAll</li>
     * <li>get</li>
     * <li>getAll</li>
     * </ul>
     *
     * @param valsToCheck Array of values to check.
     */
    @SuppressWarnings("unchecked")
    protected void checkBasicCacheOperations(Serializable... valsToCheck) throws Exception {
        assert valsToCheck.length > 0;

        // In case of BigDecimal, cache internally changes bitLength of BigDecimal's intValue,
        // so that EqualsBuilder.reflectionEquals returns false.
        // As a result in case of BigDecimal data type Objects.equals is used.
        // Same is about BigInteger.
        BiFunction<Object, Object, Boolean> equalsProcessor =
            valsToCheck[0] instanceof BigDecimal || valsToCheck[0] instanceof BigInteger ?
                Objects::equals :
                (lhs, rhs) -> EqualsBuilder.reflectionEquals(
                    lhs, rhs, false, lhs.getClass(), true);

        String cacheName = "cache" + UUID.randomUUID();

        IgniteCache<Object, Object> cache = grid(new Random().nextInt(NODES_CNT)).createCache(
            new CacheConfiguration<>()
                .setName(cacheName)
                .setAtomicityMode(atomicityMode)
                .setCacheMode(cacheMode)
                .setExpiryPolicyFactory(ttlFactory)
                .setBackups(backups)
                .setEvictionPolicyFactory(evictionFactory)
                .setOnheapCacheEnabled(evictionFactory != null || onheapCacheEnabled)
                .setWriteSynchronizationMode(writeSyncMode)
                .setAffinity(new RendezvousAffinityFunction(false, PARTITIONS_CNT)));

        Map<Serializable, Serializable> keyValMap = new HashMap<>();

        for (int i = 0; i < valsToCheck.length; i++)
            keyValMap.put(valsToCheck[i], valsToCheck[valsToCheck.length - i - 1]);

        for (Map.Entry<Serializable, Serializable> keyValEntry : keyValMap.entrySet()) {
            // Put.
            cache.put(keyValEntry.getKey(), keyValEntry.getValue());

            Serializable clonedKey = SerializationUtils.clone(keyValEntry.getKey());

            if (writeSyncMode == CacheWriteSynchronizationMode.FULL_ASYNC &&
                !waitForCondition(() -> cache.get(clonedKey) != null, TIMEOUT_FOR_KEY_RETRIEVAL_IN_FULL_ASYNC_MODE))
                fail("Unable to retrieve value for key = [" + clonedKey + "].");

            // Check Put/Get.
            assertTrue(equalsProcessor.apply(keyValEntry.getValue(), cache.get(clonedKey)));

            // Remove.
            cache.remove(clonedKey);

            if (writeSyncMode == CacheWriteSynchronizationMode.FULL_ASYNC &&
                !waitForCondition(() -> cache.get(clonedKey) == null, TIMEOUT_FOR_KEY_RETRIEVAL_IN_FULL_ASYNC_MODE))
                fail("Unable to retrieve null value for key = [" + clonedKey + "] after entry removal.");

            // Check remove.
            assertNull(cache.get(clonedKey));
        }

        // PutAll
        cache.putAll(keyValMap);

        // Check putAll.
        for (Map.Entry<Serializable, Serializable> keyValEntry : keyValMap.entrySet()) {
            Serializable clonedKey = SerializationUtils.clone(keyValEntry.getKey());

            if (writeSyncMode == CacheWriteSynchronizationMode.FULL_ASYNC &&
                !waitForCondition(() -> cache.get(clonedKey) != null, TIMEOUT_FOR_KEY_RETRIEVAL_IN_FULL_ASYNC_MODE))
                fail("Unable to retrieve value for key = [" + clonedKey + "].");

            assertTrue(equalsProcessor.apply(keyValEntry.getValue(), cache.get(clonedKey)));
        }

        Set<Serializable> clonedKeySet =
            keyValMap.keySet().stream().map(SerializationUtils::clone).collect(Collectors.toSet());

        if (writeSyncMode == CacheWriteSynchronizationMode.FULL_ASYNC &&
            !waitForCondition(() -> cache.getAll(clonedKeySet) != null, TIMEOUT_FOR_KEY_RETRIEVAL_IN_FULL_ASYNC_MODE))
            fail("Unable to retrieve values value for keySet = [" + clonedKeySet + "].");

        // Check get all.
        Map<Object, Object> mapToCheck = cache.getAll(clonedKeySet);

        assertEquals(keyValMap.size(), mapToCheck.size());

        for (Map.Entry<Serializable, Serializable> keyValEntry : keyValMap.entrySet()) {
            boolean keyFound = false;
            for (Map.Entry<Object, Object> keyValEntryToCheck : mapToCheck.entrySet()) {
                if (equalsProcessor.apply(keyValEntry.getKey(), keyValEntryToCheck.getKey())) {
                    keyFound = true;

                    assertTrue(equalsProcessor.apply(keyValEntry.getValue(), keyValEntryToCheck.getValue()));

                    break;
                }
            }
            assertTrue(keyFound);
        }

        // Remove all.
        cache.removeAll(clonedKeySet);

        // Check remove all.
        for (Serializable clonedKey : clonedKeySet) {
            if (writeSyncMode == CacheWriteSynchronizationMode.FULL_ASYNC &&
                !waitForCondition(() -> cache.get(clonedKey) == null, TIMEOUT_FOR_KEY_RETRIEVAL_IN_FULL_ASYNC_MODE))
                fail("Unable to retrieve null value for key = [" + clonedKey + "] after entry removal.");

            assertNull(cache.get(clonedKey));
        }
    }
}

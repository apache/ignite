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

package org.apache.ignite.internal.processors.cache.binary;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.spi.discovery.tcp.TestTcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils.DiscoveryHook;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Abstract tests for discovery message exchange, that is performed
 * upon binary type registration.
 */
public abstract class AbstractBinaryMetadataRegistrationTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME = "cache";

    /** Counter of new binary types. Used to generate new type name for each test. */
    private static final AtomicInteger TYPES_COUNT = new AtomicInteger();

    /**
     * Number of {@link MetadataUpdateProposedMessage} that have been sent since a test was start.
     */
    private static final AtomicInteger proposeMsgNum = new AtomicInteger();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TestTcpDiscoverySpi)cfg.getDiscoverySpi()).discoveryHook(new DiscoveryHook() {
            @Override public void beforeDiscovery(DiscoveryCustomMessage customMsg) {
                if (customMsg instanceof MetadataUpdateProposedMessage)
                    proposeMsgNum.incrementAndGet();
            }
        });

        cfg.setCacheConfiguration(cacheConfiguration());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        proposeMsgNum.set(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * Creates cache configuration with name
     * {@link AbstractBinaryMetadataRegistrationTest#CACHE_NAME CACHE_NAME}
     * and default parameters.
     */
    protected CacheConfiguration<Integer, Object> cacheConfiguration() {
        return new CacheConfiguration<>(CACHE_NAME);
    }

    /** */
    protected abstract void put(IgniteCache<Integer, Object> cache, Integer key, Object val);

    /**
     * Tests registration of user classes.
     */
    @Test
    public void testMetadataRegisteredOnceForUserClass() {
        checkMetadataRegisteredOnce(new TestValue(1));
    }

    /**
     * Tests type registration upon writing binary objects to a cache.
     */
    @Test
    public void testMetadataRegisteredOnceForBinaryObject() {
        BinaryObjectBuilder builder = grid().binary().builder("TestBinaryType");

        builder.setField("testField", 1);

        checkMetadataRegisteredOnce(builder.build());
    }

    /**
     * Tests registration of {@link Binarylizable} user classes.
     */
    @Test
    public void testMetadataRegisteredOnceForBinarylizable() {
        checkMetadataRegisteredOnce(new TestBinarylizableValue(1));
    }

    /**
     * Tests registration of {@link Externalizable} user classes.
     */
    @Test
    public void testMetadataRegisteredOnceForExternalizable() {
        checkMetadataRegisteredOnce(new TestExternalizableValue(1));
    }

    /**
     * Tests registration of enums.
     */
    @Test
    public void testMetadataRegisteredOnceForEnum() {
        checkMetadataRegisteredOnce(TestEnum.ONE);
    }

    /**
     * Checks that only one {@link MetadataUpdateProposedMessage} is sent to discovery when a binary type is
     * registered.
     *
     * @param val Value to insert into a cache to trigger type registration.
     */
    private void checkMetadataRegisteredOnce(Object val) {
        IgniteCache<Integer, Object> cache = grid().getOrCreateCache("cache");

        put(cache, 1, val);

        assertEquals("Unexpected number of MetadataUpdateProposedMessages have been received.",
            1, proposeMsgNum.get());

        put(cache, 2, val);

        assertEquals("Unexpected number of MetadataUpdateProposedMessages have been received.",
            1, proposeMsgNum.get());

        BinaryObjectBuilder builder = grid().binary().builder("OneMoreType_" + TYPES_COUNT.incrementAndGet());

        builder.setField("f", 1);

        put(cache, 3, builder.build());

        assertEquals("Unexpected number of MetadataUpdateProposedMessages have been received.",
            2, proposeMsgNum.get());
    }

    /**
     * A dummy class for testing of metadata registration.
     */
    private static class TestValue {
        /** */
        int val;

        /**
         * @param val Value.
         */
        TestValue(int val) {
            this.val = val;
        }
    }

    /**
     * A dummy {@link Binarylizable} class for testing of metadata registration.
     */
    private static class TestBinarylizableValue implements Binarylizable {
        /** */
        int val;

        /**
         * @param val Value.
         */
        TestBinarylizableValue(int val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
            writer.writeInt("val", val);
        }

        /** {@inheritDoc} */
        @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
            this.val = reader.readInt("val");
        }
    }

    /**
     * A dummy {@link Externalizable} class for testing of metadata registration.
     */
    private static class TestExternalizableValue implements Externalizable {
        /** */
        int val;

        /**
         * @param val Value.
         */
        TestExternalizableValue(int val) {
            this.val = val;
        }

        /** */
        public TestExternalizableValue() {
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(val);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            this.val = in.readInt();
        }
    }

    /**
     * A enum for testing of metadata registration.
     */
    private enum TestEnum {
        /** */
        ONE,
        /** */
        TWO,
        /** */
        THREE
    }
}

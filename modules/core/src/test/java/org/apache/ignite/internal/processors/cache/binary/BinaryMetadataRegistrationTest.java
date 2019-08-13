/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.DiscoverySpiListener;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 * Test for discovery message exchange, that is performed upon binary type registration.
 */
public class BinaryMetadataRegistrationTest extends GridCommonAbstractTest {
    /**
     * Number of {@link MetadataUpdateProposedMessage} that have been sent since a test was start.
     */
    private static final AtomicInteger proposeMsgNum = new AtomicInteger();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        GridTestUtils.DiscoveryHook discoveryHook = new GridTestUtils.DiscoveryHook() {
            @Override public void handleDiscoveryMessage(DiscoverySpiCustomMessage msg) {
                DiscoveryCustomMessage customMsg = msg == null ? null
                    : (DiscoveryCustomMessage)IgniteUtils.field(msg, "delegate");

                if (customMsg instanceof MetadataUpdateProposedMessage)
                    proposeMsgNum.incrementAndGet();
            }
        };

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi() {
            @Override public void setListener(@Nullable DiscoverySpiListener lsnr) {
                super.setListener(GridTestUtils.DiscoverySpiListenerWrapper.wrap(lsnr, discoveryHook));
            }
        };

        cfg.setDiscoverySpi(discoSpi);

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

        cache.put(1, val);

        assertEquals("Unexpected number of MetadataUpdateProposedMessages have been received.",
            1, proposeMsgNum.get());
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

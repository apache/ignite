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

package org.apache.ignite.internal.binary;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.MarshallerContextAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.MarshallerContext;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.IgniteTestResources;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests IGNITE_USE_LOCAL_BINARY_MARSHALLER_CACHE property.
 */
public class BinaryMarshallerLocalMetadataCache extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName,
        IgniteTestResources rsrcs) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName, rsrcs);

        cfg.setMarshaller(new BinaryMarshallerWrapper());

        cfg.setCacheConfiguration(new CacheConfiguration().setName("part").setBackups(1));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_USE_LOCAL_BINARY_MARSHALLER_CACHE, "true");

        startGrid(0);
        startGrid(1);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        try {
            stopAllGrids();
        }
        finally {
            System.clearProperty(IgniteSystemProperties.IGNITE_USE_LOCAL_BINARY_MARSHALLER_CACHE);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testLocalMetadata() throws Exception {
        final CyclicBarrier bar = new CyclicBarrier(64);

        awaitPartitionMapExchange();

        GridTestUtils.runMultiThreaded(new Callable<Object>() {
            @Override public Object call() throws Exception {
                bar.await();

                return grid(0).binary().toBinary(new OptimizedContainer(new Optimized()));
            }
        }, 64, "async-runner");

        // We expect 3 here because Externalizable classes are registered twice with different type IDs.
        assertEquals(3, ((BinaryMarshallerWrapper)grid(0).configuration().getMarshaller()).registerClassCalled.get());
    }

    /**
     *
     */
    private static class OptimizedContainer {
        /** */
        private Optimized optim;

        /**
         * @param optim Val.
         */
        public OptimizedContainer(Optimized optim) {
            this.optim = optim;
        }
    }

    /**
     *
     */
    private static class Optimized implements Externalizable {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** */
        private String fld;

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            U.writeUTFStringNullable(out, fld);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            fld = U.readUTFStringNullable(in);
        }
    }

    private static class BinaryMarshallerWrapper extends BinaryMarshaller {
        private MarshallerContext ctx0 = new MarshallerContextAdapter(Collections.<PluginProvider>emptyList()) {
            @Override protected boolean registerClassName(int id, String clsName) {
                U.dumpStack(id + " " + clsName);
                registerClassCalled.incrementAndGet();

                return true;
            }

            @Override protected String className(int id) throws IgniteCheckedException {
                return null;
            }
        };

        private AtomicInteger registerClassCalled = new AtomicInteger();

        /** {@inheritDoc} */
        @Override public MarshallerContext getContext() {
            return ctx0;
        }
    }
}

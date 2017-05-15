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
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
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

        cfg.setMarshaller(new BinaryMarshaller());

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
        final BinaryObject obj = grid(0).binary().toBinary(new OptimizedContainer(new Optimized()));

        ClusterGroup remotes = grid(0).cluster().forRemotes();

        OptimizedContainer res = grid(0).compute(remotes).call(new IgniteCallable<OptimizedContainer>() {
            @Override public OptimizedContainer call() throws Exception {

                return obj.deserialize();
            }
        });

        OptimizedContainer res2 = grid(0).compute(remotes).call(new IgniteCallable<OptimizedContainer>() {
            @Override public OptimizedContainer call() throws Exception {

                return obj.deserialize();
            }
        });

        System.out.println(res);
        System.out.println(res2);
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
}

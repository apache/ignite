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

import java.util.List;
import java.util.stream.Collectors;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class BinaryMetadataRegisterClassTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME = "cache";

    /** Max retry count for remove type. */
    private static final int MAX_RETRY_COUNT = 10;

    /** */
    private IgniteClient[] thinClients;

    /** */
    private List<IgniteBinary> testBinary;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(new CacheConfiguration().setName(CACHE_NAME));

        return cfg;
    }

    /** */
    protected void startCluster() throws Exception {
        startGrid("srv0");
        startGrid("srv1");
        startClientGrid("cli0");

        thinClients = new IgniteClient[] {
            Ignition.startClient(new ClientConfiguration().setAddresses("127.0.0.1:10800")),
            Ignition.startClient(new ClientConfiguration().setAddresses("127.0.0.1:10800"))
        };

        testBinary = G.allGrids().stream().map(Ignite::binary).collect(Collectors.toList());

        testBinary.add(thinClients[0].binary());
        testBinary.add(0, thinClients[1].binary());
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startCluster();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        for (IgniteClient thinClient : thinClients)
            thinClient.close();

        super.afterTest();
    }

    /** */
    @Test
    public void test() throws Exception {
        for (IgniteBinary bin : testBinary) {
            BinaryType binType = bin.registerClass(TestValue.class);

            U.sleep(1000);

            BinaryObjectBuilder bob = bin.builder(binType.typeName());
            bob.setField("intField", "str value");

            // Check that metadata is registered (the registered type of the 'intField' is Integer)
            GridTestUtils.assertThrowsAnyCause(log,
                () -> {
                    bob.build();

                    return null;
                },
                BinaryObjectException.class, "Wrong value has been set"
            );

            removeType(binType.typeName());
        }
    }

    /**
     * @param typeName Binary type name.
     */
    protected void removeType(String typeName) throws Exception {
        IgniteEx ign = ((IgniteEx)F.first(G.allGrids()));

        Exception err = null;

        for (int i = 0; i < MAX_RETRY_COUNT; ++i) {
            try {
                ign.context().cacheObjects().removeType(ign.context().cacheObjects().typeId(typeName));

                err = null;

                break;
            }
            catch (Exception e) {
                err = e;

                U.sleep(200);
            }
        }

        if (err != null)
            throw err;

        U.sleep(1000);
    }

    /** */
    public static class TestValue {
        /** */
        private int intField;

        /** */
        private double dblField;

        /** */
        private String strField;
    }
}

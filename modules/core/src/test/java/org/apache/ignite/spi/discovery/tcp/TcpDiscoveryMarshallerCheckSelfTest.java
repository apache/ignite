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

package org.apache.ignite.spi.discovery.tcp;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test for {@link TcpDiscoverySpi}.
 */
@RunWith(JUnit4.class)
public class TcpDiscoveryMarshallerCheckSelfTest extends GridCommonAbstractTest {
    /** */
    private static boolean sameMarsh;

    /** */
    private static boolean flag;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg =  super.getConfiguration(igniteInstanceName);

        cfg.setLocalHost("127.0.0.1");

        if (flag)
            cfg.setMarshaller(new JdkMarshaller());
        else
            cfg.setMarshaller(sameMarsh ? new JdkMarshaller() : new BinaryMarshaller());

        // Flip flag.
        flag = !flag;

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        flag = false;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMarshallerInConsistency() throws Exception {
        sameMarsh = false;

        startGrid(1);

        try {
            startGrid(2);

            fail("Expected SPI exception was not thrown.");
        }
        catch (IgniteCheckedException e) {
            Throwable ex = e.getCause().getCause();

            assertTrue(ex instanceof IgniteSpiException);
            assertTrue(ex.getMessage().contains("Local node's marshaller differs from remote node's marshaller"));
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMarshallerConsistency() throws Exception {
        sameMarsh = true;

        startGrid(1);
        startGrid(2);
    }
}

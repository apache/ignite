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

import org.apache.ignite.IgniteBinary;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.testframework.config.GridTestProperties.BINARY_MARSHALLER_USE_SIMPLE_NAME_MAPPER;

/**
 * Tests testing framewrok, epecially BINARY_MARSHALLER_USE_SIMPLE_NAME_MAPPER test property.
 */
public class BinarySimpleNameTestPropertySelfTest extends GridCommonAbstractTest {
    /**
     * flag for facade disabled test. As we use binary marshaller by default al
     */
    private boolean enableJdkMarshaller;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        final IgniteConfiguration configuration = super.getConfiguration(igniteInstanceName);
        if (enableJdkMarshaller)
            configuration.setMarshaller(new JdkMarshaller());
        return configuration;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPropertyEnabled() throws Exception {
        String useSimpleNameBackup = GridTestProperties.getProperty(BINARY_MARSHALLER_USE_SIMPLE_NAME_MAPPER);

        try {
            GridTestProperties.setProperty(BINARY_MARSHALLER_USE_SIMPLE_NAME_MAPPER, "true");

            checkProperty("TestClass");
        }
        finally {
            GridTestProperties.setProperty(BINARY_MARSHALLER_USE_SIMPLE_NAME_MAPPER, useSimpleNameBackup);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPropertyDisabled() throws Exception {
        checkProperty("org.ignite.test.TestClass");
    }

    /**
     * Check if Binary facade is disabled test. Test uses JDK marshaller to provide warranty facade is not available
     * @throws Exception If failed.
     */
    @Test
    public void testBinaryDisabled() throws Exception {
        enableJdkMarshaller = true;
        assertNull(startGrid().binary());
    }

    /**
     * @param expTypeName Type name.
     * @throws Exception If failed.
     */
    private void checkProperty(String expTypeName) throws Exception {
        IgniteBinary binary = startGrid().binary();

        BinaryObjectBuilder builder = binary.builder("org.ignite.test.TestClass");

        BinaryObject bObj = builder.build();

        assertEquals(expTypeName, bObj.type().typeName());
    }
}

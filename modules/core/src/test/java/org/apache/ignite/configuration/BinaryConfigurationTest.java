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

package org.apache.ignite.configuration;

import org.apache.ignite.internal.InstanceFactory;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;

/**
 * Binary configuration tests.
 */
public class BinaryConfigurationTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testGetInstanceFactory() throws Exception {
        BinaryConfiguration binaryConfiguration = binaryConfiguration();

        assertNotNull(binaryConfiguration.getInstanceFactory(SimpleObject.class));
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoveInstanceFactory() throws Exception {
        BinaryConfiguration binaryConfiguration = binaryConfiguration();

        assertNotNull(binaryConfiguration.getInstanceFactory(SimpleObject.class));

        binaryConfiguration.removeInstanceFactory(SimpleObject.class);

        assertNull(binaryConfiguration.getInstanceFactory(SimpleObject.class));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutInstanceFactory() throws Exception {
        BinaryConfiguration binaryConfiguration = binaryConfiguration();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutInstanceFactoryPredefinedType() throws Exception {
        BinaryConfiguration binaryConfiguration = binaryConfiguration();

        try {
            binaryConfiguration.putInstanceFactory(String.class, new InstanceFactory() {
                @NotNull @Override public Object newInstance() {
                    return "StringInstanceFactory";
                }
            });
        }
        catch (AssertionError error) {
            assertTrue(error.getMessage().contains("java.lang.String"));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testResetInitializationFactory() throws Exception {
        BinaryConfiguration binaryConfiguration = binaryConfiguration();

        assertNotNull(binaryConfiguration.getInstanceFactory(SimpleObject.class));

        binaryConfiguration.resetInitializationFactory();

        assertNull(binaryConfiguration.getInstanceFactory(SimpleObject.class));
    }

    /**
     * @return new {@link BinaryConfiguration} with InstanceFactory of SimpleObject
     */
    private BinaryConfiguration binaryConfiguration() {
        BinaryConfiguration binaryConfiguration = new BinaryConfiguration();

        binaryConfiguration.putInstanceFactory(SimpleObject.class, new InstanceFactory() {
            @NotNull @Override public Object newInstance() {
                return new SimpleObject();
            }
        });

        return binaryConfiguration;
    }

    /** */
    private static class SimpleObject {
    }
}
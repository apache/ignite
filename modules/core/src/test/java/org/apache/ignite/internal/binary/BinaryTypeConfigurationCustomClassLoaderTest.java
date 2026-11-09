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

import java.net.URL;
import java.util.Collections;
import org.apache.ignite.Ignite;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.marshaller.ClassLoaderUtils;
import org.apache.ignite.testframework.GridTestExternalClassLoader;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Checks that a key class visible only through {@link IgniteConfiguration#setClassLoader(ClassLoader)} gets its
 * {@link AffinityKeyMapped} field recognised regardless of whether the type is listed in
 * {@link BinaryConfiguration#setTypeConfigurations(java.util.Collection)}.
 */
public class BinaryTypeConfigurationCustomClassLoaderTest extends GridCommonAbstractTest {
    /** Key class name. The class lives in the {@code extdata/p2p} module and is not available on the test classpath. */
    private static final String KEY_CLS = "org.apache.ignite.tests.p2p.CacheDeploymentAffinityMappedKey";

    /** Name of the field annotated with {@link AffinityKeyMapped} in the key class. */
    private static final String AFF_FIELD = "aff";

    /** Loader that sees the key class. Set as {@link IgniteConfiguration#setClassLoader(ClassLoader)}. */
    private static ClassLoader ldr;

    /** Whether to list the key class in {@link BinaryConfiguration#setTypeConfigurations(java.util.Collection)}. */
    private boolean cfgType;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        ldr = new GridTestExternalClassLoader(new URL[] {new URL(GridTestProperties.getProperty("p2p.uri.cls"))});

        // Preconditions: the class is visible through the configured loader only.
        assertNull(ClassLoaderUtils.classForName(KEY_CLS));
        assertEquals(AFF_FIELD, BinaryUtils.affinityFieldName(ldr.loadClass(KEY_CLS)));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setClassLoader(ldr);

        if (cfgType) {
            cfg.setBinaryConfiguration(
                new BinaryConfiguration().setTypeConfigurations(Collections.singletonList(new BinaryTypeConfiguration(KEY_CLS))));
        }

        return cfg;
    }

    /** Type is not listed in the binary configuration: the class is examined when an instance is marshalled. */
    @Test
    public void testAffinityKeyFieldOfUnconfiguredType() throws Exception {
        cfgType = false;

        Ignite ignite = startGrid(0);

        BinaryObject bo = ignite.binary().toBinary(newKey());

        assertEquals(AFF_FIELD, bo.type().affinityKeyFieldName());
        assertEquals(AFF_FIELD, ignite.binary().type(KEY_CLS).affinityKeyFieldName());
    }

    /** Type is listed in the binary configuration: the class is examined on node start. */
    @Test
    public void testAffinityKeyFieldOfConfiguredType() throws Exception {
        cfgType = true;

        Ignite ignite = startGrid(0);

        assertEquals(AFF_FIELD, ignite.binary().type(KEY_CLS).affinityKeyFieldName());

        BinaryObject bo = ignite.binary().toBinary(newKey());

        assertEquals(AFF_FIELD, bo.type().affinityKeyFieldName());
    }

    /** @return New instance of the key class. */
    private static Object newKey() throws Exception {
        return ldr.loadClass(KEY_CLS).getDeclaredConstructor().newInstance();
    }

}

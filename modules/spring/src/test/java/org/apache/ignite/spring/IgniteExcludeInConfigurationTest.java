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

package org.apache.ignite.spring;

import java.net.URL;
import java.util.Collection;
import org.apache.ignite.cache.CacheTypeMetadata;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.spring.IgniteSpringHelper;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.internal.IgniteComponentType.SPRING;

/**
 * Checks excluding properties, beans with not existing classes in spring.
 */
public class IgniteExcludeInConfigurationTest extends GridCommonAbstractTest {
    private URL cfgLocation = U.resolveIgniteUrl(
        "modules/spring/src/test/java/org/apache/ignite/spring/sprint-exclude.xml");

    /** Spring should exclude properties by list and ignore beans with class not existing in classpath. */
    public void testExclude() throws Exception {
         IgniteSpringHelper spring = SPRING.create(false);

        Collection<IgniteConfiguration> cfgs = spring.loadConfigurations(cfgLocation, "fileSystemConfiguration",
            "typeMetadata").get1();

        assertNotNull(cfgs);
        assertEquals(1, cfgs.size());

        IgniteConfiguration cfg = cfgs.iterator().next();

        assertEquals(1, cfg.getCacheConfiguration().length);
        assertNull(cfg.getCacheConfiguration()[0].getTypeMetadata());

        assertNull(cfg.getFileSystemConfiguration());

        cfgs = spring.loadConfigurations(cfgLocation, "keyType").get1();

        assertNotNull(cfgs);
        assertEquals(1, cfgs.size());

        cfg = cfgs.iterator().next();

        assertEquals(1, cfg.getCacheConfiguration().length);

        Collection<CacheTypeMetadata> typeMetadatas = cfg.getCacheConfiguration()[0].getTypeMetadata();

        assertEquals(1, typeMetadatas.size());
        assertNull(typeMetadatas.iterator().next().getKeyType());
    }

    /** Spring should fail if bean class not exist in classpath. */
    public void testFail() throws Exception {
        IgniteSpringHelper spring = SPRING.create(false);

        try {
             assertNotNull(spring.loadConfigurations(cfgLocation).get1());
        } catch (Exception e) {
            assertTrue(X.hasCause(e, ClassNotFoundException.class));
        }
    }
}
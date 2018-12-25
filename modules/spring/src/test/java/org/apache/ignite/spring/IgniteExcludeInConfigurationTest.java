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
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.spring.IgniteSpringHelper;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.internal.IgniteComponentType.SPRING;

/**
 * Checks excluding properties, beans with not existing classes in spring.
 */
@RunWith(JUnit4.class)
public class IgniteExcludeInConfigurationTest extends GridCommonAbstractTest {
    private URL cfgLocation = U.resolveIgniteUrl(
        "modules/spring/src/test/java/org/apache/ignite/spring/sprint-exclude.xml");

    /** Spring should exclude properties by list and ignore beans with class not existing in classpath. */
    @Test
    public void testExclude() throws Exception {
         IgniteSpringHelper spring = SPRING.create(false);

        Collection<IgniteConfiguration> cfgs = spring.loadConfigurations(cfgLocation, "fileSystemConfiguration",
            "queryEntities").get1();

        assertNotNull(cfgs);
        assertEquals(1, cfgs.size());

        IgniteConfiguration cfg = cfgs.iterator().next();

        assertEquals(1, cfg.getCacheConfiguration().length);

        assertTrue(F.isEmpty(cfg.getCacheConfiguration()[0].getQueryEntities()));

        assertNull(cfg.getFileSystemConfiguration());

        cfgs = spring.loadConfigurations(cfgLocation, "keyType").get1();

        assertNotNull(cfgs);
        assertEquals(1, cfgs.size());

        cfg = cfgs.iterator().next();

        assertEquals(1, cfg.getCacheConfiguration().length);

        Collection<QueryEntity> queryEntities = cfg.getCacheConfiguration()[0].getQueryEntities();

        assertEquals(1, queryEntities.size());
        assertNull(queryEntities.iterator().next().getKeyType());
    }

    /** Spring should fail if bean class not exist in classpath. */
    @Test
    public void testFail() throws Exception {
        IgniteSpringHelper spring = SPRING.create(false);

        try {
             assertNotNull(spring.loadConfigurations(cfgLocation).get1());
        } catch (Exception e) {
            assertTrue(X.hasCause(e, ClassNotFoundException.class));
        }
    }
}

/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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

import static org.apache.ignite.internal.IgniteComponentType.SPRING;

/**
 * Checks excluding properties, beans with not existing classes in spring.
 */
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

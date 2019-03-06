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

package org.apache.ignite.util;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests for {@link AttributeNodeFilter}.
 */
public class AttributeNodeFilterSelfTest extends GridCommonAbstractTest {
    /** */
    private Map<String, ?> attrs;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (attrs != null)
            cfg.setUserAttributes(attrs);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        attrs = null;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSingleAttribute() throws Exception {
        IgnitePredicate<ClusterNode> filter = new AttributeNodeFilter("attr", "value");

        assertTrue(filter.apply(nodeProxy(F.asMap("attr", "value"))));
        assertFalse(filter.apply(nodeProxy(F.asMap("attr", "wrong"))));
        assertFalse(filter.apply(nodeProxy(F.asMap("attr", null))));
        assertFalse(filter.apply(nodeProxy(Collections.<String, Object>emptyMap())));
        assertFalse(filter.apply(nodeProxy(F.asMap("wrong", "value"))));
        assertFalse(filter.apply(nodeProxy(F.asMap("null", "value"))));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSingleAttributeNullValue() throws Exception {
        IgnitePredicate<ClusterNode> filter = new AttributeNodeFilter("attr", null);

        assertTrue(filter.apply(nodeProxy(F.asMap("attr", null))));
        assertTrue(filter.apply(nodeProxy(Collections.<String, Object>emptyMap())));
        assertTrue(filter.apply(nodeProxy(F.asMap("wrong", "value"))));
        assertTrue(filter.apply(nodeProxy(F.asMap("wrong", null))));
        assertFalse(filter.apply(nodeProxy(F.asMap("attr", "value"))));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultipleAttributes() throws Exception {
        IgnitePredicate<ClusterNode> filter =
            new AttributeNodeFilter(F.<String, Object>asMap("attr1", "value1", "attr2", "value2"));

        assertTrue(filter.apply(nodeProxy(F.asMap("attr1", "value1", "attr2", "value2"))));
        assertFalse(filter.apply(nodeProxy(F.asMap("attr1", "wrong", "attr2", "value2"))));
        assertFalse(filter.apply(nodeProxy(F.asMap("attr1", "value1", "attr2", "wrong"))));
        assertFalse(filter.apply(nodeProxy(F.asMap("attr1", "wrong", "attr2", "wrong"))));
        assertFalse(filter.apply(nodeProxy(F.asMap("attr1", "value1"))));
        assertFalse(filter.apply(nodeProxy(F.asMap("attr2", "value2"))));
        assertFalse(filter.apply(nodeProxy(Collections.<String, Object>emptyMap())));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultipleAttributesNullValues() throws Exception {
        IgnitePredicate<ClusterNode> filter = new AttributeNodeFilter(F.asMap("attr1", null, "attr2", null));

        assertTrue(filter.apply(nodeProxy(F.asMap("attr1", null, "attr2", null))));
        assertTrue(filter.apply(nodeProxy(F.asMap("attr1", null))));
        assertTrue(filter.apply(nodeProxy(F.asMap("attr2", null))));
        assertTrue(filter.apply(nodeProxy(Collections.<String, Object>emptyMap())));
        assertFalse(filter.apply(nodeProxy(F.asMap("attr1", "value1"))));
        assertFalse(filter.apply(nodeProxy(F.asMap("attr2", "value2"))));
        assertFalse(filter.apply(nodeProxy(F.asMap("attr1", "value1", "attr2", "value2"))));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClusterGroup() throws Exception {
        Ignite group1 = startGridsMultiThreaded(3);

        attrs = F.asMap("group", "data");

        Ignite group2 = startGridsMultiThreaded(3, 2);

        assertEquals(2, group1.cluster().forPredicate(new AttributeNodeFilter("group", "data")).nodes().size());
        assertEquals(2, group2.cluster().forPredicate(new AttributeNodeFilter("group", "data")).nodes().size());

        assertEquals(3, group1.cluster().forPredicate(new AttributeNodeFilter("group", null)).nodes().size());
        assertEquals(3, group2.cluster().forPredicate(new AttributeNodeFilter("group", null)).nodes().size());

        assertEquals(0, group1.cluster().forPredicate(new AttributeNodeFilter("group", "wrong")).nodes().size());
        assertEquals(0, group2.cluster().forPredicate(new AttributeNodeFilter("group", "wrong")).nodes().size());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCacheFilter() throws Exception {
        Ignite group1 = startGridsMultiThreaded(3);

        attrs = F.asMap("group", "data");

        Ignite group2 = startGridsMultiThreaded(3, 2);

        group1.createCache(new CacheConfiguration<>("test-cache").
            setNodeFilter(new AttributeNodeFilter("group", "data")));

        assertEquals(2, group1.cluster().forDataNodes("test-cache").nodes().size());
        assertEquals(2, group2.cluster().forDataNodes("test-cache").nodes().size());

        assertEquals(0, group1.cluster().forDataNodes("wrong").nodes().size());
        assertEquals(0, group2.cluster().forDataNodes("wrong").nodes().size());
    }

    /**
     * @param attrs Attributes.
     * @return Node proxy.
     */
    private static ClusterNode nodeProxy(final Map<String, ?> attrs) {
        return (ClusterNode)Proxy.newProxyInstance(
            ClusterNode.class.getClassLoader(),
            new Class[] { ClusterNode.class },
            new InvocationHandler() {
                @Override public Object invoke(Object proxy, Method mtd, Object[] args) throws Throwable {
                    if ("attributes".equals(mtd.getName()))
                        return attrs;

                    throw new UnsupportedOperationException();
                }
            });
    }
}

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

package org.apache.ignite.internal;

import org.apache.ignite.IgniteSpringBean;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test for {@link org.apache.ignite.IgniteSpringBean} serialization.
 */
@RunWith(JUnit4.class)
public class GridSpringBeanSerializationSelfTest extends GridCommonAbstractTest {
    /** Marshaller. */
    private static Marshaller marsh;

    /** Attribute key. */
    private static final String ATTR_KEY = "checkAttr";

    /** Bean. */
    private static IgniteSpringBean bean;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        IgniteConfiguration cfg = config();

        marsh = createStandaloneBinaryMarshaller(cfg);

        bean = new IgniteSpringBean();

        bean.setConfiguration(cfg);

        bean.afterSingletonsInstantiated();
    }

    /**
     * @return Grid configuration.
     */
    private IgniteConfiguration config() {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setUserAttributes(F.asMap(ATTR_KEY, true));

        cfg.setConnectorConfiguration(null);

        cfg.setIgniteInstanceName(getTestIgniteInstanceName());

        cfg.setBinaryConfiguration(new BinaryConfiguration());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        bean.destroy();

        bean = null;
        marsh = null;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSerialization() throws Exception {
        assert bean != null;

        IgniteSpringBean bean0 = marsh.unmarshal(marsh.marshal(bean), null);

        assert bean0 != null;
        assert bean0.log() != null;
        assert bean0.cluster().localNode() != null;
        assert bean0.cluster().localNode().<Boolean>attribute(ATTR_KEY);
    }
}

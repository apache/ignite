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

import java.util.Date;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.util.lang.GridTuple3;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.mxbean.IgniteMXBean;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * This test will test node local storage.
 */
@GridCommonTest(group = "Kernal Self")
@RunWith(JUnit4.class)
public class GridNodeLocalSelfTest extends GridCommonAbstractTest {
    /** Create test. */
    public GridNodeLocalSelfTest() {
        super(/* Start grid. */true);
    }

    /**
     * Test node-local values operations.
     *
     * @throws Exception If test failed.
     */
    @Test
    public void testNodeLocal() throws Exception {
        Ignite g = G.ignite(getTestIgniteInstanceName());

        String keyStr = "key";
        int keyNum = 1;
        Date keyDate = new Date();

        GridTuple3 key = F.t(keyNum, keyStr, keyDate);

        ConcurrentMap<Object, Object> nl = g.cluster().nodeLocalMap();

        nl.put(keyStr, "Hello world!");
        nl.put(key, 12);

        assert nl.containsKey(keyStr);
        assert nl.containsKey(key);
        assert !nl.containsKey(keyNum);
        assert !nl.containsKey(F.t(keyNum, keyStr));

        assert "Hello world!".equals(nl.get(keyStr));
        assert (Integer)nl.get(key) == 12;
    }

    /**
     * Test that node local map is cleared via {@link IgniteMXBean#clearNodeLocalMap()}.
     *
     * @throws Exception if test failed.
     */
    @Test
    public void testClearNodeLocalMap() throws Exception {
        final String key = "key";
        final String value = "value";

        Ignite grid = G.ignite(getTestIgniteInstanceName());

        ConcurrentMap<Object, Object> nodeLocalMap = grid.cluster().nodeLocalMap();
        nodeLocalMap.put(key, value);

        assert !nodeLocalMap.isEmpty() : "Empty node local map";
        assert nodeLocalMap.containsKey(key);

        IgniteMXBean igniteMXBean = (IgniteMXBean)grid;
        igniteMXBean.clearNodeLocalMap();
        assert nodeLocalMap.isEmpty() : "Not empty node local map";
    }
}

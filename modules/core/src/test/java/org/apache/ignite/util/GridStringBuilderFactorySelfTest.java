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

import org.apache.ignite.internal.util.GridStringBuilderFactory;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.junit.Test;

/**
 * String builder factory test.
 */
@GridCommonTest(group = "Utils")
public class GridStringBuilderFactorySelfTest extends GridCommonAbstractTest {
    /** */
    public GridStringBuilderFactorySelfTest() {
        super(/*start grid*/false);
    }

    /**
     * Tests string builder factory.
     */
    @Test
    public void testStringBuilderFactory() {
        SB b1 = GridStringBuilderFactory.acquire();

        assert b1.length() == 0;

        b1.a("B1 Test String");

        SB b2 = GridStringBuilderFactory.acquire();

        assert b2.length() == 0;

        assert b1 != b2;

        b1.a("B2 Test String");

        assert !b1.toString().equals(b2.toString());

        GridStringBuilderFactory.release(b2);
        GridStringBuilderFactory.release(b1);

        assert b1.length() == 0;
        assert b2.length() == 0;

        SB b3 = GridStringBuilderFactory.acquire();

        assert b1 == b3;

        assert b3.length() == 0;

        b3.a("B3 Test String");

        GridStringBuilderFactory.release(b3);

        assert b3.length() == 0;
    }
}

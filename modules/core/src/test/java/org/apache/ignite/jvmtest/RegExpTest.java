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

package org.apache.ignite.jvmtest;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;

/**
 * Java reg exp test.
 */
public class RegExpTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRegExp() throws Exception {
        String normal =
            "swap-spaces/space1/b53b3a3d6ab90ce0268229151c9bde11|b53b3a3d6ab90ce0268229151c9bde11|1315392441288";

        byte[] b1 = new byte[200];
        byte[] b2 = normal.getBytes();

        U.arrayCopy(b2, 0, b1, 30, b2.length);

        CharSequence corrupt = new String(b1);

        String ptrn = "[a-z0-9/\\-]+\\|[a-f0-9]+\\|[0-9]+";

        Pattern p = Pattern.compile(ptrn);

        Matcher matcher = p.matcher(corrupt);

        assert matcher.find();

        X.println(String.valueOf(matcher.start()));

        assert normal.matches(ptrn);
    }
}

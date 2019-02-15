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

package org.apache.ignite.internal.processors.rest;

import org.apache.ignite.internal.util.typedef.F;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test REST with enabled authentication and token.
 */
@RunWith(JUnit4.class)
public class JettyRestProcessorAuthenticationWithTokenSelfTest extends JettyRestProcessorAuthenticationAbstractTest {
    /** */
    private String tok = "";

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        // Authenticate and extract token.
        if (F.isEmpty(tok)) {
            String ret = content(null, GridRestCommand.AUTHENTICATE,
                "user", DFLT_USER,
                "password", DFLT_PWD);

            int p1 = ret.indexOf("sessionToken");
            int p2 = ret.indexOf('"', p1 + 16);

            tok = ret.substring(p1 + 15, p2);
        }
    }

    /** {@inheritDoc} */
    @Override protected String restUrl() {
        String url = super.restUrl();

        if (!F.isEmpty(tok))
            url += "sessionToken=" + tok + "&";

        return url;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInvalidSessionToken() throws Exception {
        tok = null;

        String ret = content(null, GridRestCommand.VERSION);

        assertResponseContainsError(ret, "Failed to handle request - session token not found or invalid");

        tok = "InvalidToken";

        ret = content(null, GridRestCommand.VERSION);

        assertResponseContainsError(ret, "Failed to handle request - session token not found or invalid");

        tok = "26BE027D32CC42329DEC92D517B44E9E";

        ret = content(null, GridRestCommand.VERSION);

        assertResponseContainsError(ret, "Failed to handle request - unknown session token (maybe expired session)");

        tok = null; // Cleanup token for next tests.
    }
}

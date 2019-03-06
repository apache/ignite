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

import java.util.concurrent.Callable;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.authentication.IgniteAccessControlException;
import org.apache.ignite.internal.processors.authentication.IgniteAuthenticationProcessor;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.configuration.WALMode.NONE;

/**
 * Test REST with enabled authentication.
 */
public abstract class JettyRestProcessorAuthenticationAbstractTest extends JettyRestProcessorUnsignedSelfTest {
    /** */
    protected static final String DFLT_USER = "ignite";

    /** */
    protected static final String DFLT_PWD = "ignite";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        U.resolveWorkDirectory(U.defaultWorkDirectory(), "db", true);

        super.beforeTestsStarted();
    }

    /** {@inheritDoc} */
    @Override protected boolean securityEnabled() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected void initCache() {
        // We need to activate cluster.
        grid(0).cluster().active(true);

        super.initCache();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setAuthenticationEnabled(true);

        // For now persistence is needed because of current authentication implementation.
        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setMaxSize(100 * 1024 * 1024))
            .setWalMode(NONE);

        DataRegionConfiguration testDataRegionCfg = new DataRegionConfiguration();
        testDataRegionCfg.setName("testDataRegion");
        testDataRegionCfg.setMaxSize(10 * 1024 * 1024);

        DataRegionConfiguration authDataRegionCfg = new DataRegionConfiguration();
        authDataRegionCfg.setName("authDataRegion")
            .setMaxSize(10 * 1024 * 1024)
            .setPersistenceEnabled(true);

        dsCfg.setDataRegionConfigurations(testDataRegionCfg, authDataRegionCfg);

        cfg.setDataStorageConfiguration(dsCfg);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAuthenticationCommand() throws Exception {
        String ret = content(null, GridRestCommand.AUTHENTICATE);

        assertResponseSucceeded(ret, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAddUpdateRemoveUser() throws Exception {
        // Add user.
        String ret = content(null, GridRestCommand.ADD_USER,
            "user", "user1",
            "password", "password1");

        JsonNode res = validateJsonResponse(ret);

        assertTrue(res.asBoolean());

        IgniteAuthenticationProcessor auth = grid(0).context().authentication();

        assertNotNull(auth.authenticate("user1", "password1"));

        // Update user password.
        ret = content(null, GridRestCommand.UPDATE_USER,
            "user", "user1",
            "password", "password2");

        res = validateJsonResponse(ret);

        assertTrue(res.asBoolean());

        assertNotNull(auth.authenticate("user1", "password2"));

        // Remove user.
        ret = content(null, GridRestCommand.REMOVE_USER,
            "user", "user1");

        res = validateJsonResponse(ret);

        assertTrue(res.asBoolean());

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                grid(0).context().authentication().authenticate("user1", "password1");

                return null;
            }
        }, IgniteAccessControlException.class, "The user name or password is incorrect");
    }
}

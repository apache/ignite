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

package org.apache.ignite.internal.processors.rest;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.authentication.IgniteAuthenticationProcessor;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.processors.authentication.User.DFAULT_USER_NAME;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.ADD_USER;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.REMOVE_USER;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.UPDATE_USER;

/**
 * Tests authorization of user management commands executed through REST in case {@link IgniteAuthenticationProcessor}
 * is used as the Ignite security implementation.
 */
public class JettyRestProcessorAuthenticatorUserManagementAuthorizationTest extends JettyRestProcessorCommonSelfTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setAuthenticationEnabled(true);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setMaxSize(100 * 1024 * 1024)
                .setPersistenceEnabled(true)));

        return cfg;
    }

    /** Tests authorization of user management commands executed through REST. */
    @Test
    public void testUserManagementAuthorization() throws Exception {
        startGrid().cluster().state(ACTIVE);

        String dfltUserPwd = "ignite";
        String userLogin = "user";
        String userPwd = "pwd";

        checkRestRequest(DFAULT_USER_NAME, dfltUserPwd, ADD_USER, userLogin, userPwd, null);

        checkRestRequest(userLogin, userPwd, ADD_USER, "not-allowed-user", userPwd,
            "User management operations are not allowed for user. [curUser=user]");

        checkRestRequest(userLogin, userPwd, UPDATE_USER, DFAULT_USER_NAME, "new-pwd",
            "User management operations are not allowed for user. [curUser=user]");

        checkRestRequest(DFAULT_USER_NAME, dfltUserPwd, UPDATE_USER, DFAULT_USER_NAME, "new-pwd", null);
        dfltUserPwd = "new-pwd";

        checkRestRequest(userLogin, userPwd, UPDATE_USER, userLogin, "new-pwd", null);

        checkRestRequest(DFAULT_USER_NAME, dfltUserPwd, UPDATE_USER, userLogin, "pwd", null);

        checkRestRequest(userLogin, userPwd, REMOVE_USER, DFAULT_USER_NAME, null,
            "User management operations are not allowed for user. [curUser=user]");

        checkRestRequest(DFAULT_USER_NAME, dfltUserPwd, REMOVE_USER, DFAULT_USER_NAME, null,
            "Default user cannot be removed.");

        checkRestRequest(DFAULT_USER_NAME, dfltUserPwd, REMOVE_USER, userLogin, null, null);
    }

    /** Checks REST request execution. */
    public void checkRestRequest(
        String login,
        String pwd,
        GridRestCommand cmd,
        String loginParam,
        String pwdParam,
        String expErr
    ) throws Exception {
        JsonNode res = JSON_MAPPER.readTree(content(null, cmd,
            "ignite.login", login,
            "ignite.password", pwd,
            "user", loginParam,
            "password", pwdParam == null ? "" : pwdParam));

        if (expErr == null) {
            assertEquals(0, res.get("successStatus").intValue());
            assertNull(res.get("error").textValue());
        }
        else
            assertTrue(res.get("error").textValue().contains(expErr));
    }

    /** {@inheritDoc} */
    @Override protected String signature() {
        return null;
    }
}

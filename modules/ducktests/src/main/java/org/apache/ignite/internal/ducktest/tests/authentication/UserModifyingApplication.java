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

package org.apache.ignite.internal.ducktest.tests.authentication;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;
import org.apache.ignite.internal.processors.authentication.AuthorizationContext;

/**
 * Simple application that modify users
 * Posible operations:
 * 0 - Add User
 * 1 - Update User
 * 2 - Remove User
 */

public class UserModifyingApplication extends IgniteAwareApplication {

    @Override public void run(JsonNode jsonNode) {

        int operation = jsonNode.get("operation").asInt();
        String authUsername = jsonNode.get("auth_username").asText();
        String authPassword = jsonNode.get("auth_password").asText();
        String username = jsonNode.get("username").asText();
        String password = jsonNode.get("password").asText();

        IgniteEx igniteEx = (IgniteEx) ignite;
        try {
            AuthorizationContext actx = igniteEx.context().authentication().authenticate(authUsername, authPassword);
            AuthorizationContext.context(actx);
        } catch (IgniteCheckedException e) {
            log.info("Exception while authenticating." + e);
        }

        markInitialized();

        switch (operation) {
            case 0:
                try {
                    log.info("Adding user " + username + " with password " + password);
                    igniteEx.context().authentication().addUser(username, password);
                } catch (IgniteCheckedException e) {
                    log.info("Exception while adding user." + e);
                }
                break;
            case 1:
                try {
                    log.info("Update user " + username + " change password " + password);
                    igniteEx.context().authentication().updateUser(username, password);
                } catch (IgniteCheckedException e) {
                    log.info("Exception while updating user." + e);
                }
                break;
            case 2:
                try {
                    log.info("Remove user " + username);
                    igniteEx.context().authentication().removeUser(username);
                } catch (IgniteCheckedException e) {
                    log.info("Exception while removing user." + e);
                }
                break;
        }

        markFinished();
    }
}

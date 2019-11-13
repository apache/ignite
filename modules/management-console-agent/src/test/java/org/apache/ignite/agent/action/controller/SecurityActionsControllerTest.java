/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.agent.action.controller;

import java.util.UUID;
import org.apache.ignite.agent.dto.action.AuthenticateCredentials;
import org.apache.ignite.agent.dto.action.Request;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.junit.Test;

import static org.apache.ignite.agent.dto.action.ActionStatus.COMPLETED;
import static org.apache.ignite.agent.dto.action.ActionStatus.FAILED;
import static org.apache.ignite.agent.dto.action.ResponseError.AUTHENTICATION_ERROR_CODE;

/**
 * Security actions controller test.
 */
public class SecurityActionsControllerTest extends AbstractActionControllerWithAuthenticationTest {
    /**
     * Should authenticate with login and password.
     */
    @Test
    public void shouldSuccessfullyAuthenticate() {
        Request req = new Request()
            .setId(UUID.randomUUID())
            .setAction("SecurityActions.authenticate")
            .setArgument(new AuthenticateCredentials().setCredentials(new SecurityCredentials("ignite", "ignite")));

        executeAction(req, (r) -> r.getStatus() == COMPLETED && UUID.fromString((String) r.getResult()) != null);
    }

    /**
     * Should send error response when user don't provide credentials.
     */
    @Test
    public void shouldSendErrorResponseWithEmptyCredentials() {
        Request req = new Request()
            .setId(UUID.randomUUID())
            .setAction("SecurityActions.authenticate")
            .setArgument(new AuthenticateCredentials());

        executeAction(req, (r) -> r.getStatus() == FAILED && r.getError().getCode() == AUTHENTICATION_ERROR_CODE);
    }

    /**
     * Should send error response when user provide invalid credentials.
     */
    @Test
    public void shouldSendErrorResponseWithInvalidCredentials() {
        Request req = new Request()
            .setId(UUID.randomUUID())
            .setAction("SecurityActions.authenticate")
            .setArgument(new AuthenticateCredentials().setCredentials(new SecurityCredentials("ignite", "ignite2")));

        executeAction(req, (r) -> r.getStatus() == FAILED && r.getError().getCode() == AUTHENTICATION_ERROR_CODE);
    }
}

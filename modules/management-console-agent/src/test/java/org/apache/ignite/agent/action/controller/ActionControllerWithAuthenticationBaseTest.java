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
 * Action controller base test with authentication.
 */
public class ActionControllerWithAuthenticationBaseTest extends AbstractActionControllerWithAuthenticationTest {
    /**
     * Should authenticate and execute action.
     */
    @Test
    public void shouldExecuteActionWithAuthentication() {
        UUID sesId = authenticate(new AuthenticateCredentials().setCredentials(new SecurityCredentials("ignite", "ignite")));

        Request req = new Request()
            .setId(UUID.randomUUID())
            .setAction("IgniteTestActionController.numberAction")
            .setArgument(10)
            .setSessionId(sesId);

        executeAction(req, (r) -> r.getStatus() == COMPLETED);
    }

    /**
     * Should send error response when user don't provide session id for executing secure action.
     */
    @Test
    public void shouldSendErrorResponseOnExecutingSecuredActionWithoutAuthentication() {
        Request req = new Request()
            .setId(UUID.randomUUID())
            .setAction("IgniteTestActionController.numberAction")
            .setArgument(10);

        executeAction(req, (r) -> r.getStatus() == FAILED && r.getError().getCode() == AUTHENTICATION_ERROR_CODE);
    }

    /**
     * Should send error response when user provide invalid session id for executing secure action.
     */
    @Test
    public void shouldSendErrorResponseOnExecutingSecuredActionWithInvalidSessionId() {
        Request req = new Request()
            .setId(UUID.randomUUID())
            .setAction("IgniteTestActionController.numberAction")
            .setArgument(10)
            .setSessionId(UUID.randomUUID());

        executeAction(req, (r) -> r.getStatus() == FAILED && r.getError().getCode() == AUTHENTICATION_ERROR_CODE);
    }
}

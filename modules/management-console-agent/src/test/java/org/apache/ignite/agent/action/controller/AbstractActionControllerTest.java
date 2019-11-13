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

import java.util.concurrent.Callable;
import java.util.function.Function;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ignite.agent.AgentCommonAbstractTest;
import org.apache.ignite.agent.dto.action.Request;
import org.apache.ignite.agent.dto.action.Response;
import org.apache.ignite.internal.IgniteEx;
import org.junit.Before;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.agent.StompDestinationsUtils.buildActionRequestTopic;
import static org.apache.ignite.agent.StompDestinationsUtils.buildActionResponseDest;
import static org.apache.ignite.agent.utils.AgentObjectMapperFactory.jsonMapper;
import static org.awaitility.Awaitility.with;

/**
 * Abstract test for action controllers.
 */
abstract class AbstractActionControllerTest extends AgentCommonAbstractTest {
    /** Mapper. */
    protected final ObjectMapper mapper = jsonMapper();

    /**
     * Start grid.
     */
    @Before
    public void startup() throws Exception {
        IgniteEx ignite = (IgniteEx) startGrid();
        changeManagementConsoleUri(ignite);

        cluster = ignite.cluster();

        cluster.active(true);
    }

    /**
     * Send action request and check execution result with assert function and specific grid instances count.
     *
     * @param req      Request.
     * @param assertFn Assert fn.
     */
    protected void executeAction(Request req, Function<Response, Boolean> assertFn) {
        assertWithPoll(
            () -> interceptor.isSubscribedOn(buildActionRequestTopic(cluster.id()))
        );

        template.convertAndSend(buildActionRequestTopic(cluster.id()), req);

        assertWithPoll(
            () -> {
                Response res = interceptor.getPayload(buildActionResponseDest(cluster.id(), req.getId()), Response.class);
                return res != null && assertFn.apply(res);
            }
        );
    }

    /** {@inheritDoc} */
    @Override protected void assertWithPoll(Callable<Boolean> cond) {
        with().pollInterval(500, MILLISECONDS).await().atMost(10, SECONDS).until(cond);
    }
}

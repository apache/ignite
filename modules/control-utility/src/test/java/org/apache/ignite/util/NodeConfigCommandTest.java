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

package org.apache.ignite.util;

import java.util.UUID;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.commandline.CommandList;
import org.apache.ignite.internal.commandline.config.NodeConfigCommandArg;
import org.junit.Test;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_INVALID_ARGUMENTS;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.commandline.CommandList.NODE_CONFIG;
import static org.apache.ignite.internal.commandline.config.NodeConfigCommandArg.NODE_ID;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;

/** Tests output of {@link CommandList#NODE_CONFIG} command. */
public class NodeConfigCommandTest extends GridCommandHandlerClusterByClassAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        injectTestSystemOut();
    }

    /** Tests command error output in case value of {@link NodeConfigCommandArg#NODE_ID} argument is invalid. */
    @Test
    public void testInvalidNodeIdFailure() {
        assertEquals(
            EXIT_CODE_INVALID_ARGUMENTS,
            execute(NODE_CONFIG.text(), NODE_ID.argName(), "invalid_node_id")
        );

        assertContains(
            log,
            testOut.toString(),
            "Failed to parse " + NODE_ID.argName() +
                " command argument. String representation of \"java.util.UUID\" is exepected." +
                " For example: 123e4567-e89b-42d3-a456-556642440000"
        );
    }

    /** Tests command error output in case value of {@link NodeConfigCommandArg#NODE_ID} argument is invalid. */
    @Test
    public void testUnknownNodeIdFailure() {
        String unknownNodeId = UUID.randomUUID().toString();

        assertEquals(
            EXIT_CODE_INVALID_ARGUMENTS,
            execute(NODE_CONFIG.text(), NODE_ID.argName(), unknownNodeId)
        );

        assertContains(
            log,
            testOut.toString(),
            "Node with id=" + unknownNodeId + " not found"
        );
    }

    /** Tests command error output in case value of {@link NodeConfigCommandArg#NODE_ID} argument is invalid. */
    @Test
    public void testNodeConfigOutput() {
        IgniteEx ignite = ignite(0);

        assertEquals(
            EXIT_CODE_OK,
            execute(NODE_CONFIG.text(), NODE_ID.argName(), ignite.localNode().id().toString())
        );

        assertContains(
            log,
            testOut.toString(),
            ignite.localNode().id().toString()
        );

        assertContains(
            log,
            testOut.toString(),
            ignite.localNode().consistentId().toString()
        );
    }
}

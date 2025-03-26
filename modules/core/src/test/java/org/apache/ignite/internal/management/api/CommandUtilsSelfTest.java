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

package org.apache.ignite.internal.management.api;

import org.apache.ignite.internal.management.SystemViewCommand;
import org.junit.Test;

import static org.apache.ignite.internal.management.api.CommandUtils.CMD_WORDS_DELIM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/** */
public class CommandUtilsSelfTest {
    /** */
    private static final String CMD_NAME = "SystemView";

    /** */
    private static final String FORMATTED_CMD_NAME = "system-view";

    /** */
    @Test
    public void testToFormatted() {
        String[][] pairs = new String[][] {
            {"my-command", "myCommand"},
            {"node-ids", "nodeIds"},
            {"node-id", "nodeId"},
            {FORMATTED_CMD_NAME, CMD_NAME}
        };

        for (String[] pair : pairs)
            assertEquals(pair[0], CommandUtils.toFormattedName(pair[1], CMD_WORDS_DELIM));

        assertEquals(CMD_NAME, CommandUtils.fromFormattedCommandName(FORMATTED_CMD_NAME, CMD_WORDS_DELIM));
    }

    /** */
    @Test
    public void testAsOptional() {
        assertEquals("[my-param]", CommandUtils.asOptional("my-param", true));
        assertEquals("my-param", CommandUtils.asOptional("my-param", false));
    }

    /** */
    @Test
    public void testCommandName() {
        assertEquals(FORMATTED_CMD_NAME, CommandUtils.toFormattedCommandName(SystemViewCommand.class));
    }

    /** */
    @Test
    public void testFromCommandName() {
        assertEquals("ConfigureHistogram", CommandUtils.fromFormattedCommandName("configure-histogram", CMD_WORDS_DELIM));
    }

    /**  */
    @Test
    public void testLoadExternalCommands() {
        assertFalse(CommandUtils.loadExternalCommands().isEmpty());
    }
}

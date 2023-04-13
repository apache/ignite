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

package org.apache.ignite.internal.commands;

import org.junit.Test;
import static org.apache.ignite.internal.commandline.CommandUtils.CMD_WORDS_DELIM;
import static org.apache.ignite.internal.commandline.CommandUtils.formattedName;
import static org.junit.Assert.assertEquals;

/** */
public class CommandUtilsSelfTest {
    /** */
    @Test
    public void testToCommand() {
        assertEquals("my-command", formattedName("myCommand", CMD_WORDS_DELIM));
        assertEquals("node-ids", formattedName("nodeIDs", CMD_WORDS_DELIM));
        assertEquals("node-id", formattedName("nodeID", CMD_WORDS_DELIM));
        assertEquals("system-view", formattedName("SystemView", CMD_WORDS_DELIM));
    }
}

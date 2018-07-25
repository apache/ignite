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

package org.apache.ignite.tensorflow.submitter.parser;

import org.apache.ignite.tensorflow.submitter.command.StartCommand;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link StartCommandParser}.
 */
public class StartCommandParserTest {
    /** Command parser. */
    private final StartCommandParser parser = new StartCommandParser(() -> null);

    /** */
    @Test
    public void testParseCorrectArgs() {
        Runnable cmd = parser.parse(new String[]{ "TEST_CACHE", "FOLDER", "python3", "test.py" });

        assertNotNull(cmd);
        assertTrue(cmd instanceof StartCommand);

        StartCommand startCmd = (StartCommand) cmd;
        assertEquals("TEST_CACHE", startCmd.getUpstreamCacheName());
        assertEquals("FOLDER", startCmd.getJobArchivePath());
        assertEquals(2, startCmd.getCommands().length);
        assertEquals("python3", startCmd.getCommands()[0]);
        assertEquals("test.py", startCmd.getCommands()[1]);
    }

    /** */
    @Test
    public void testParseIncorrectArgs() {
        Runnable cmd = parser.parse(new String[]{ "TEST_CACHE", "42" });

        assertNull(cmd);
    }
}

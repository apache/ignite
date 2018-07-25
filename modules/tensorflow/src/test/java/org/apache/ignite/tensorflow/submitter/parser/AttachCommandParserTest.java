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

import java.util.UUID;
import org.apache.ignite.tensorflow.submitter.command.AttachCommand;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link AttachCommandParser}.
 */
public class AttachCommandParserTest {
    /** Command parser. */
    private final AttachCommandParser parser = new AttachCommandParser(() -> null);

    /** */
    @Test
    public void testParseCorrectArgs() {
        UUID id = UUID.randomUUID();

        Runnable cmd = parser.parse(new String[]{ id.toString() });

        assertNotNull(cmd);
        assertTrue(cmd instanceof AttachCommand);

        AttachCommand attachCmd = (AttachCommand) cmd;

        assertEquals(id, attachCmd.getClusterId());
    }

    /** */
    @Test
    public void testParseIncorrectArgs() {
        Runnable cmd = parser.parse(new String[]{});

        assertNull(cmd);
    }
}

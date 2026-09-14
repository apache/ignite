/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.tools.compatibility.messages;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;

/** Tests the message table command. */
public class MessageTableTest {
    /** Temporary output directory. */
    @Rule public TemporaryFolder tmp = new TemporaryFolder();

    /** The command writes the collected table and preserves the checked-in format. */
    @Test public void testMain() throws Exception {
        Path out = tmp.getRoot().toPath().resolve("result/table.xml");

        MessageTable.main(new String[] {out.toString()});

        String table = Files.readString(out);

        assertEquals(Files.readString(Path.of("src/main/resources/messages/table.xml")), table);

        try (var files = Files.list(out.getParent())) {
            assertEquals(1, files.count());
        }
    }
}

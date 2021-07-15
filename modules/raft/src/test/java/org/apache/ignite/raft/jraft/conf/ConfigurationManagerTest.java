/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.conf;

import org.apache.ignite.raft.jraft.entity.LogId;
import org.apache.ignite.raft.jraft.test.TestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConfigurationManagerTest {
    private ConfigurationManager confManager;

    @BeforeEach
    public void setup() {
        this.confManager = new ConfigurationManager();
    }

    @Test
    public void testGetStuff() {
        ConfigurationEntry lastConf = this.confManager.getLastConfiguration();
        ConfigurationEntry snapshot = this.confManager.getSnapshot();
        assertSame(snapshot, lastConf);
        assertSame(snapshot, this.confManager.get(0));

        ConfigurationEntry confEntry1 = TestUtils.getConfEntry("localhost:8080", null);
        confEntry1.setId(new LogId(0, 0));
        assertTrue(this.confManager.add(confEntry1));
        lastConf = this.confManager.getLastConfiguration();
        assertNotSame(snapshot, lastConf);
        assertSame(confEntry1, lastConf);

        assertSame(confEntry1, this.confManager.get(0));
        assertSame(confEntry1, this.confManager.get(1));
        assertSame(confEntry1, this.confManager.get(2));

        ConfigurationEntry confEntry2 = TestUtils.getConfEntry("localhost:8080,localhost:8081", "localhost:8080");
        confEntry2.setId(new LogId(1, 1));
        assertTrue(this.confManager.add(confEntry2));

        lastConf = this.confManager.getLastConfiguration();
        assertNotSame(snapshot, lastConf);
        assertSame(confEntry2, lastConf);

        assertSame(confEntry1, this.confManager.get(0));
        assertSame(confEntry2, this.confManager.get(1));
        assertSame(confEntry2, this.confManager.get(2));

        ConfigurationEntry confEntry3 = TestUtils.getConfEntry("localhost:8080,localhost:8081,localhost:8082",
            "localhost:8080,localhost:8081");
        confEntry3.setId(new LogId(2, 1));
        assertTrue(this.confManager.add(confEntry3));

        lastConf = this.confManager.getLastConfiguration();
        assertNotSame(snapshot, lastConf);
        assertSame(confEntry3, lastConf);

        assertSame(confEntry1, this.confManager.get(0));
        assertSame(confEntry2, this.confManager.get(1));
        assertSame(confEntry3, this.confManager.get(2));
    }

    private ConfigurationEntry createEnry(int index) {
        ConfigurationEntry configurationEntry = new ConfigurationEntry();
        configurationEntry.getId().setIndex(index);
        return configurationEntry;
    }

    @Test
    public void testTruncate() {
        for (int i = 0; i < 10; i++) {
            assertTrue(this.confManager.add(createEnry(i)));
        }

        assertEquals(9, this.confManager.getLastConfiguration().getId().getIndex());
        assertEquals(5, this.confManager.get(5).getId().getIndex());
        assertEquals(6, this.confManager.get(6).getId().getIndex());
        this.confManager.truncatePrefix(6);
        //truncated, so is snapshot index
        assertEquals(0, this.confManager.get(5).getId().getIndex());
        assertEquals(6, this.confManager.get(6).getId().getIndex());
        assertEquals(9, this.confManager.getLastConfiguration().getId().getIndex());

        this.confManager.truncateSuffix(7);
        assertEquals(0, this.confManager.get(5).getId().getIndex());
        assertEquals(6, this.confManager.get(6).getId().getIndex());
        assertEquals(7, this.confManager.getLastConfiguration().getId().getIndex());
        assertEquals(7, this.confManager.get(9).getId().getIndex());
    }
}

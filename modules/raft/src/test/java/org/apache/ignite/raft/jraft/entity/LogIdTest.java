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
package org.apache.ignite.raft.jraft.entity;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LogIdTest {

    @Test
    public void testCompareTo() {
        LogId logId = new LogId();
        assertEquals(0, logId.getIndex());
        assertEquals(0, logId.getTerm());

        assertTrue(new LogId(1, 0).compareTo(logId) > 0);
        assertTrue(new LogId(0, 1).compareTo(logId) > 0);

        logId = new LogId(1, 2);
        assertTrue(new LogId(0, 1).compareTo(logId) < 0);
        assertTrue(new LogId(0, 2).compareTo(logId) < 0);
        assertTrue(new LogId(3, 1).compareTo(logId) < 0);
        assertTrue(new LogId(1, 2).compareTo(logId) == 0);
    }

    @Test
    public void testChecksum() {
        LogId logId = new LogId();
        logId.setIndex(1);
        logId.setTerm(2);
        long c = logId.checksum();
        assertTrue(c != 0);
        assertEquals(c, logId.checksum());
    }
}

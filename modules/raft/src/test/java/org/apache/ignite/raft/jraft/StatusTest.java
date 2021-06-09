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
package org.apache.ignite.raft.jraft;

import org.apache.ignite.raft.jraft.error.RaftError;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class StatusTest {

    @Test
    public void testOKStatus() {
        Status s = new Status();
        assertTrue(s.isOk());
        assertEquals(0, s.getCode());
        assertNull(s.getErrorMsg());
    }

    @Test
    public void testStatusOK() {
        Status s = Status.OK();
        assertTrue(s.isOk());
        assertEquals(0, s.getCode());
        assertNull(s.getErrorMsg());
        assertNotSame(Status.OK(), s);
    }

    @Test
    public void testNewStatus() {
        Status s = new Status(-2, "test");
        assertEquals(-2, s.getCode());
        assertEquals("test", s.getErrorMsg());
        assertFalse(s.isOk());
    }

    @Test
    public void testNewStatusVaridicArgs() {
        Status s = new Status(-2, "test %s %d", "world", 100);
        assertEquals(-2, s.getCode());
        assertEquals("test world 100", s.getErrorMsg());
        assertFalse(s.isOk());
    }

    @Test
    public void testNewStatusRaftError() {
        Status s = new Status(RaftError.EACCES, "test %s %d", "world", 100);
        assertEquals(RaftError.EACCES.getNumber(), s.getCode());
        assertEquals(RaftError.EACCES, s.getRaftError());
        assertEquals("test world 100", s.getErrorMsg());
        assertFalse(s.isOk());
    }

    @Test
    public void testSetErrorRaftError() {
        Status s = new Status();
        s.setError(RaftError.EACCES, "test %s %d", "world", 100);
        assertEquals(RaftError.EACCES.getNumber(), s.getCode());
        assertEquals(RaftError.EACCES, s.getRaftError());
        assertEquals("test world 100", s.getErrorMsg());
        assertFalse(s.isOk());
    }

    @Test
    public void testSetError() {
        Status s = new Status();
        s.setError(RaftError.EACCES.getNumber(), "test %s %d", "world", 100);
        assertEquals(RaftError.EACCES.getNumber(), s.getCode());
        assertEquals(RaftError.EACCES, s.getRaftError());
        assertEquals("test world 100", s.getErrorMsg());
        assertFalse(s.isOk());
    }

}

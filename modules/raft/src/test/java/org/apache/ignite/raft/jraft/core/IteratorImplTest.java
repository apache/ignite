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
package org.apache.ignite.raft.jraft.core;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.raft.jraft.Closure;
import org.apache.ignite.raft.jraft.JRaftUtils;
import org.apache.ignite.raft.jraft.StateMachine;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.entity.EnumOutter;
import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.storage.LogManager;
import org.apache.ignite.raft.jraft.util.Utils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(value = MockitoJUnitRunner.class)
public class IteratorImplTest {
    private IteratorImpl iter;

    @Mock
    private StateMachine fsm;

    @Mock
    private LogManager logManager;

    private List<Closure> closures;

    private AtomicLong applyingIndex;

    @Before
    public void setup() {
        this.applyingIndex = new AtomicLong(0);
        this.closures = new ArrayList<>();
        for (int i = 0; i < 11; i++) {
            this.closures.add(new MockClosure());
            final LogEntry log = new LogEntry(EnumOutter.EntryType.ENTRY_TYPE_NO_OP);
            log.getId().setIndex(i);
            log.getId().setTerm(1);
            Mockito.when(this.logManager.getEntry(i)).thenReturn(log);
        }
        NodeOptions nodeOptions = new NodeOptions();
        nodeOptions.setCommonExecutor(JRaftUtils.createExecutor("test-executor", Utils.cpus()));
        this.iter = new IteratorImpl(fsm, logManager, closures, 0L, 0L, 10L, applyingIndex, nodeOptions);
    }

    @Test
    public void testPredicates() {
        assertTrue(this.iter.isGood());
        assertFalse(this.iter.hasError());
    }

    @Test
    public void testNext() {
        int i = 1;
        while (iter.isGood()) {
            assertEquals(i, iter.getIndex());
            assertNotNull(iter.done());
            final LogEntry log = iter.entry();
            assertEquals(i, log.getId().getIndex());
            assertEquals(1, log.getId().getTerm());
            iter.next();
            i++;
        }
        assertEquals(i, 11);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetErrorAndRollbackInvalid() {
        this.iter.setErrorAndRollback(-1, null);
    }

    @Test
    public void testRunTheRestClosureWithError() throws Exception {
        testSetErrorAndRollback();
        for (final Closure closure : this.closures) {
            final MockClosure mc = (MockClosure) closure;
            assertNull(mc.s);
        }

        this.iter.runTheRestClosureWithError();
        Thread.sleep(500);
        int i = 0;
        for (final Closure closure : this.closures) {
            i++;
            final MockClosure mc = (MockClosure) closure;
            if (i < 7) {
                assertNull(mc.s);
            }
            else {
                final Status s = mc.s;
                Assert.assertEquals(RaftError.ESTATEMACHINE.getNumber(), s.getCode());
                assertEquals(
                    "StateMachine meet critical error when applying one or more tasks since index=6, Status[UNKNOWN<-1>: test]",
                    s.getErrorMsg());
            }
        }
    }

    @Test
    public void testSetErrorAndRollback() {
        testNext();
        assertFalse(iter.hasError());
        this.iter.setErrorAndRollback(5, new Status(-1, "test"));
        assertTrue(iter.hasError());
        Assert.assertEquals(EnumOutter.ErrorType.ERROR_TYPE_STATE_MACHINE, iter.getError().getType());
        Assert.assertEquals(RaftError.ESTATEMACHINE.getNumber(), iter.getError().getStatus().getCode());
        Assert
            .assertEquals(
                "StateMachine meet critical error when applying one or more tasks since index=6, Status[UNKNOWN<-1>: test]",
                iter.getError().getStatus().getErrorMsg());
        assertEquals(6, iter.getIndex());
    }

}

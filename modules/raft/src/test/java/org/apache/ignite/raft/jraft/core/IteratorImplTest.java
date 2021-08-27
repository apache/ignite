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
import java.util.concurrent.ExecutorService;
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
import org.apache.ignite.raft.jraft.util.ExecutorServiceHelper;
import org.apache.ignite.raft.jraft.util.Utils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class IteratorImplTest {
    private IteratorImpl iter;

    @Mock
    private StateMachine fsm;

    @Mock
    private LogManager logManager;

    private List<Closure> closures;

    private AtomicLong applyingIndex;

    private ExecutorService executor;

    @BeforeEach
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
        executor = JRaftUtils.createExecutor("test-executor", Utils.cpus());
        nodeOptions.setCommonExecutor(executor);
        this.iter = new IteratorImpl(fsm, logManager, closures, 0L, 0L, 10L, applyingIndex, nodeOptions);
    }

    @AfterEach
    public void teardown() {
        ExecutorServiceHelper.shutdownAndAwaitTermination(executor);
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

    @Test
    public void testSetErrorAndRollbackInvalid() {
        assertThrows(IllegalArgumentException.class, () -> iter.setErrorAndRollback(-1, null));
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
                assertEquals(RaftError.ESTATEMACHINE.getNumber(), s.getCode());
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
        assertEquals(EnumOutter.ErrorType.ERROR_TYPE_STATE_MACHINE, iter.getError().getType());
        assertEquals(RaftError.ESTATEMACHINE.getNumber(), iter.getError().getStatus().getCode());
        assertEquals(
                "StateMachine meet critical error when applying one or more tasks since index=6, Status[UNKNOWN<-1>: test]",
                iter.getError().getStatus().getErrorMsg());
        assertEquals(6, iter.getIndex());
    }

}

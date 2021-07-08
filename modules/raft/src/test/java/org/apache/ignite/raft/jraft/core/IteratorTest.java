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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.raft.jraft.Closure;
import org.apache.ignite.raft.jraft.Iterator;
import org.apache.ignite.raft.jraft.StateMachine;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.entity.EnumOutter;
import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.storage.LogManager;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class IteratorTest {

    private IteratorImpl iterImpl;
    private Iterator iter;

    @Mock
    private StateMachine fsm;
    @Mock
    private LogManager logManager;
    private List<Closure> closures;
    private AtomicLong applyingIndex;

    @BeforeEach
    public void setup() {
        this.applyingIndex = new AtomicLong(0);
        this.closures = new ArrayList<>();
        for (int i = 0; i < 11; i++) {
            this.closures.add(new MockClosure());
            final LogEntry log = new LogEntry(EnumOutter.EntryType.ENTRY_TYPE_DATA);
            log.getId().setIndex(i);
            log.getId().setTerm(1);
            log.setData(ByteBuffer.allocate(i));
            Mockito.when(this.logManager.getEntry(i)).thenReturn(log);
        }
        this.iterImpl = new IteratorImpl(fsm, logManager, closures, 0L, 0L, 10L, applyingIndex, new NodeOptions());
        this.iter = new IteratorWrapper(iterImpl);
    }

    @Test
    public void testPredicates() {
        assertTrue(this.iter.hasNext());
    }

    @Test
    public void testNext() {
        int i = 1;
        while (iter.hasNext()) {
            assertEquals(i, iter.getIndex());
            assertNotNull(iter.done());
            assertEquals(i, iter.getIndex());
            assertEquals(1, iter.getTerm());
            assertEquals(i, iter.getData().remaining());
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
    public void testSetErrorAndRollback() {
        testNext();
        assertFalse(iterImpl.hasError());
        this.iter.setErrorAndRollback(5, new Status(-1, "test"));
        assertTrue(iterImpl.hasError());
        assertEquals(EnumOutter.ErrorType.ERROR_TYPE_STATE_MACHINE, iterImpl.getError().getType());
        assertEquals(RaftError.ESTATEMACHINE.getNumber(), iterImpl.getError().getStatus().getCode());
        assertEquals(
            "StateMachine meet critical error when applying one or more tasks since index=6, Status[UNKNOWN<-1>: test]",
            iterImpl.getError().getStatus().getErrorMsg());
        assertEquals(6, iter.getIndex());
    }
}

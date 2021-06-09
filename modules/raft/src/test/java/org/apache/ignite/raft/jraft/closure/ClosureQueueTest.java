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
package org.apache.ignite.raft.jraft.closure;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.raft.jraft.Closure;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ClosureQueueTest {
    private ClosureQueueImpl queue;

    @Before
    public void setup() {
        this.queue = new ClosureQueueImpl(new NodeOptions());
    }

    @SuppressWarnings("SameParameterValue")
    private Closure mockClosure(final CountDownLatch latch) {
        return status -> {
            if (latch != null) {
                latch.countDown();
            }
        };
    }

    @Test
    public void testAppendPop() {
        for (int i = 0; i < 10; i++) {
            this.queue.appendPendingClosure(mockClosure(null));
        }
        assertEquals(0, this.queue.getFirstIndex());
        List<Closure> closures = new ArrayList<>();
        assertEquals(0, this.queue.popClosureUntil(4, closures));
        assertEquals(5, closures.size());

        assertEquals(5, this.queue.getFirstIndex());

        closures.clear();
        assertEquals(5, this.queue.popClosureUntil(4, closures));
        assertTrue(closures.isEmpty());
        assertEquals(4, this.queue.popClosureUntil(3, closures));
        assertTrue(closures.isEmpty());

        assertEquals(-1, this.queue.popClosureUntil(10, closures));
        assertTrue(closures.isEmpty());

        //pop remaining 5 elements
        assertEquals(5, this.queue.popClosureUntil(9, closures));
        assertEquals(5, closures.size());
        assertEquals(10, this.queue.getFirstIndex());
        closures.clear();
        assertEquals(2, this.queue.popClosureUntil(1, closures));
        assertTrue(closures.isEmpty());
        assertEquals(4, this.queue.popClosureUntil(3, closures));
        assertTrue(closures.isEmpty());

        for (int i = 0; i < 10; i++) {
            this.queue.appendPendingClosure(mockClosure(null));
        }

        assertEquals(10, this.queue.popClosureUntil(15, closures));
        assertEquals(6, closures.size());
        assertEquals(16, this.queue.getFirstIndex());

        assertEquals(-1, this.queue.popClosureUntil(20, closures));
        assertTrue(closures.isEmpty());
        assertEquals(16, this.queue.popClosureUntil(19, closures));
        assertEquals(4, closures.size());
        assertEquals(20, this.queue.getFirstIndex());
    }

    @Test
    public void testResetFirstIndex() {
        assertEquals(0, this.queue.getFirstIndex());
        this.queue.resetFirstIndex(10);
        assertEquals(10, this.queue.getFirstIndex());
        for (int i = 0; i < 10; i++) {
            this.queue.appendPendingClosure(mockClosure(null));
        }

        List<Closure> closures = new ArrayList<>();
        assertEquals(5, this.queue.popClosureUntil(4, closures));
        assertTrue(closures.isEmpty());
        assertEquals(4, this.queue.popClosureUntil(3, closures));
        assertTrue(closures.isEmpty());

        assertEquals(10, this.queue.popClosureUntil(19, closures));
        assertEquals(20, this.queue.getFirstIndex());
        assertEquals(10, closures.size());
        // empty ,return index+1
        assertEquals(21, this.queue.popClosureUntil(20, closures));
        assertTrue(closures.isEmpty());
    }
}

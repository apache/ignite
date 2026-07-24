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

package org.apache.ignite.internal.processors.query.calcite.message;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.ignite.internal.processors.query.calcite.exec.task.AbstractQueryTaskExecutor;
import org.apache.ignite.internal.processors.query.calcite.integration.AbstractBasicIntegrationTest;
import org.junit.Test;

import static java.util.stream.Collectors.toList;

/**
 * Verifies that payload of calcite messages is unmarshalled on the query task executor, not on a NIO thread: probe
 * objects travel as {@code QueryStartRequest} parameters and as {@code QueryBatchMessage} rows, and record the thread
 * that deserializes them.
 */
public class CalciteMessageUnmarshalThreadIntegrationTest extends AbstractBasicIntegrationTest {
    /** */
    private static final int ROWS = 200;

    /** Names of the threads that deserialized a {@link Probe}. */
    private static final Queue<String> unmarshalThreads = new ConcurrentLinkedQueue<>();

    /** {@inheritDoc} */
    @Override protected int nodeCount() {
        return 2;
    }

    /** */
    @Test
    public void testPayloadUnmarshalledOnQueryExecutor() {
        sql("CREATE TABLE t(id INT PRIMARY KEY, oth OTHER)");

        for (int i = 0; i < ROWS; i++)
            sql("INSERT INTO t VALUES (?, ?)", i, new Probe());

        unmarshalThreads.clear();

        List<List<?>> res = sql("SELECT id, oth FROM t WHERE oth != ?", new Probe());

        assertEquals(ROWS, res.size());

        assertFalse("No probe deserialization recorded", unmarshalThreads.isEmpty());

        List<String> nioThreads = unmarshalThreads.stream().filter(t -> t.contains("nio")).collect(toList());

        assertTrue("Message payload unmarshalled on NIO threads: " + nioThreads, nioThreads.isEmpty());

        assertTrue("No probe deserialization on the query task executor: " + unmarshalThreads,
            unmarshalThreads.stream().anyMatch(t -> t.startsWith(AbstractQueryTaskExecutor.THREAD_PREFIX)));
    }

    /**
     * Records the name of the thread deserializing it. The custom {@code writeObject}/{@code readObject} pair also
     * routes the class through {@code OptimizedMarshaller}, so the hook is invoked on every unmarshal.
     */
    private static class Probe implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private void writeObject(ObjectOutputStream out) throws IOException {
            out.defaultWriteObject();
        }

        /** */
        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
            in.defaultReadObject();

            unmarshalThreads.add(Thread.currentThread().getName());
        }
    }
}

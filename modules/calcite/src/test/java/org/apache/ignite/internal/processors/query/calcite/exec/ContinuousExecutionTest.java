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

package org.apache.ignite.internal.processors.query.calcite.exec;

import com.google.common.collect.ImmutableList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.UUID;
import org.apache.ignite.internal.processors.query.calcite.trait.AllNodes;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;

/**
 *
 */
@RunWith(Parameterized.class)
public class ContinuousExecutionTest extends AbstractExecutionTest {
    /** */
    @Parameter(0)
    public int rowsCount;

    /** */
    @Parameter(1)
    public int remoteFragmentsCount;

    @Parameterized.Parameters(name = "rowsCount={0}, remoteFragmentsCount={1}")
    public static List<Object[]> parameters() {
        return ImmutableList.of(
            new Object[]{10, 1},
            new Object[]{10, 5},
            new Object[]{10, 10},
            new Object[]{100, 1},
            new Object[]{100, 5},
            new Object[]{100, 10},
            new Object[]{100_000, 1},
            new Object[]{100_000, 5},
            new Object[]{100_000, 10});
    }

    /**
     * @throws Exception If failed.
     */
    @Before
    @Override public void setup() throws Exception {
        nodesCount = remoteFragmentsCount + 1;
        super.setup();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testContinuousExecution() throws Exception {
        UUID queryId = UUID.randomUUID();

        List<UUID> nodes = nodes();

        for (int i = 1; i < nodes.size(); i++) {
            UUID localNodeId = nodes.get(i);

            Iterable<Object[]> iterable = () -> new Iterator<Object[]>() {
                /** */
                private int cntr;

                /** */
                private final Random rnd = new Random();

                /** {@inheritDoc} */
                @Override public boolean hasNext() {
                    return cntr < rowsCount;
                }

                /** {@inheritDoc} */
                @Override public Object[] next() {
                    if (cntr >= rowsCount)
                        throw new NoSuchElementException();

                    Object[] row = new Object[6];

                    for (int i = 0; i < row.length; i++)
                        row[i] = rnd.nextInt(10);

                    cntr++;

                    return row;
                }
            };

            ExecutionContext ectx = executionContext(localNodeId, queryId, 0);

            ScanNode scan = new ScanNode(ectx, iterable);
            ProjectNode project = new ProjectNode(ectx, scan, r -> new Object[]{r[0], r[1], r[5]});
            FilterNode filter = new FilterNode(ectx, project, r -> (Integer) r[0] >= 2);

            MailboxRegistry registry = mailboxRegistry(localNodeId);

            Outbox<Object[]> outbox = new Outbox<>(
                exchangeService(localNodeId),
                registry,
                ectx, 1,0, filter, new AllNodes(nodes.subList(0, 1)));

            registry.register(outbox);

            outbox.context().execute(outbox::request);
        }

        UUID localNodeId = nodes.get(0);

        ExecutionContext ectx = executionContext(localNodeId, queryId, 1);

        MailboxRegistry registry = mailboxRegistry(localNodeId);

        Inbox<Object[]> inbox = (Inbox<Object[]>) registry.register(
            new Inbox<Object[]>(exchangeService(localNodeId), registry, ectx, 0, 0));

        inbox.init(ectx, nodes.subList(1, nodes.size()), null);

        ConsumerNode node = new ConsumerNode(ectx, inbox);

        while (node.hasNext()) {
            Object[] row = node.next();

            assertTrue((Integer)row[0] >= 2);
        }
    }
}

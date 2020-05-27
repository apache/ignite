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

package org.apache.ignite.internal.processors.query.calcite.exec.rel;

import com.google.common.collect.ImmutableList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.UUID;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.MailboxRegistry;
import org.apache.ignite.internal.processors.query.calcite.trait.AllNodes;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;

/**
 *
 */
@SuppressWarnings("TypeMayBeWeakened")
@RunWith(Parameterized.class)
public class ContinuousExecutionTest extends AbstractExecutionTest {
    /** */
    @Parameter()
    public int rowsCnt;

    /** */
    @Parameter(1)
    public int remoteFragmentsCnt;

    /** */
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
            new Object[]{100_000, 10}
        );
    }

    /**
     * @throws Exception If failed.
     */
    @Before
    @Override public void setup() throws Exception {
        nodesCnt = remoteFragmentsCnt + 1;
        super.setup();
    }

    /** */
    @Test
    public void testContinuousExecution() {
        UUID qryId = UUID.randomUUID();

        List<UUID> nodes = nodes();

        for (int i = 1; i < nodes.size(); i++) {
            UUID locNodeId = nodes.get(i);

            Iterable<Object[]> iterable = () -> new Iterator<Object[]>() {
                /** */
                private int cntr;

                /** */
                private final Random rnd = new Random();

                /** {@inheritDoc} */
                @Override public boolean hasNext() {
                    return cntr < rowsCnt;
                }

                /** {@inheritDoc} */
                @Override public Object[] next() {
                    if (cntr >= rowsCnt)
                        throw new NoSuchElementException();

                    Object[] row = new Object[6];

                    for (int i = 0; i < row.length; i++)
                        row[i] = rnd.nextInt(10);

                    cntr++;

                    return row;
                }
            };

            ExecutionContext<Object[]> ectx = executionContext(locNodeId, qryId, 0);

            ScanNode<Object[]> scan = new ScanNode<>(ectx, iterable);

            ProjectNode<Object[]> project = new ProjectNode<>(ectx, r -> new Object[]{r[0], r[1], r[5]});
            project.register(scan);

            FilterNode<Object[]> filter = new FilterNode<>(ectx, r -> (Integer) r[0] >= 2);
            filter.register(project);

            MailboxRegistry registry = mailboxRegistry(locNodeId);

            Outbox<Object[]> outbox = new Outbox<>(ectx, exchangeService(locNodeId), registry,
                0, 1, new AllNodes(nodes.subList(0, 1)));

            outbox.register(filter);
            registry.register(outbox);

            outbox.context().execute(outbox::init);
        }

        UUID locNodeId = nodes.get(0);

        ExecutionContext<Object[]> ectx = executionContext(locNodeId, qryId, 1);

        MailboxRegistry registry = mailboxRegistry(locNodeId);

        Inbox<Object[]> inbox = (Inbox<Object[]>) registry.register(
            new Inbox<>(ectx, exchangeService(locNodeId), registry, 0, 0));

        inbox.init(ectx, nodes.subList(1, nodes.size()), null);

        RootNode<Object[]> node = new RootNode<>(ectx, r -> {});

        node.register(inbox);

        while (node.hasNext()) {
            Object[] row = node.next();

            assertTrue((Integer)row[0] >= 2);
        }
    }
}

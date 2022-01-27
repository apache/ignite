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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Stream;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.MailboxRegistry;
import org.apache.ignite.internal.processors.query.calcite.trait.AllNodes;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.TypeUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;

/**
 *
 */
@SuppressWarnings("TypeMayBeWeakened")
public class ContinuousExecutionTest extends AbstractExecutionTest {
    /** Row count parameter number. */
    protected static final int ROW_CNT_PARAM_NUM = LAST_PARAM_NUM + 1;

    /** Remote fragment parameter number. */
    protected static final int REMOTE_FRAGMENTS_PARAM_NUM = LAST_PARAM_NUM + 2;

    /** */
    @Parameter(ROW_CNT_PARAM_NUM)
    public int rowsCnt;

    /** */
    @Parameter(REMOTE_FRAGMENTS_PARAM_NUM)
    public int remoteFragmentsCnt;

    /** */
    @Parameterized.Parameters(name = PARAMS_STRING + ", " +
        "rowsCount={" + ROW_CNT_PARAM_NUM + "}, " +
        "remoteFragmentsCount={" + REMOTE_FRAGMENTS_PARAM_NUM + "}")
    public static List<Object[]> data() {
        List<Object[]> extraParams = new ArrayList<>();

        ImmutableList<Object[]> newParams = ImmutableList.of(
            new Object[] {10, 1},
            new Object[] {10, 5},
            new Object[] {10, 10},
            new Object[] {100, 1},
            new Object[] {100, 5},
            new Object[] {100, 10},
            new Object[] {100_000, 1},
            new Object[] {100_000, 5},
            new Object[] {100_000, 10}
        );

        for (Object[] newParam : newParams) {
            for (Object[] inheritedParam : AbstractExecutionTest.parameters()) {
                Object[] both = Stream.concat(Arrays.stream(inheritedParam), Arrays.stream(newParam))
                    .toArray(Object[]::new);

                extraParams.add(both);
            }
        }

        return extraParams;
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
            IgniteTypeFactory tf = ectx.getTypeFactory();

            RelDataType rowType = TypeUtils.createRowType(tf, int.class, int.class, int.class, int.class, int.class, int.class);
            ScanNode<Object[]> scan = new ScanNode<>(ectx, rowType, iterable);

            rowType = TypeUtils.createRowType(tf, int.class, int.class, int.class);
            ProjectNode<Object[]> project = new ProjectNode<>(ectx, rowType, r -> new Object[]{r[0], r[1], r[5]});
            project.register(scan);

            FilterNode<Object[]> filter = new FilterNode<>(ectx, rowType, r -> (Integer)r[0] >= 2);
            filter.register(project);

            MailboxRegistry registry = mailboxRegistry(locNodeId);

            Outbox<Object[]> outbox = new Outbox<>(ectx, rowType, exchangeService(locNodeId), registry,
                0, 1, new AllNodes(nodes.subList(0, 1)));

            outbox.register(filter);
            registry.register(outbox);

            outbox.context().execute(outbox::init, outbox::onError);
        }

        UUID locNodeId = nodes.get(0);

        ExecutionContext<Object[]> ectx = executionContext(locNodeId, qryId, 1);
        IgniteTypeFactory tf = ectx.getTypeFactory();

        MailboxRegistry registry = mailboxRegistry(locNodeId);

        Inbox<Object[]> inbox = (Inbox<Object[]>)registry.register(
            new Inbox<>(ectx, exchangeService(locNodeId), registry, 0, 0));

        RelDataType rowType = TypeUtils.createRowType(tf, int.class, int.class, int.class);
        inbox.init(ectx, rowType, nodes.subList(1, nodes.size()), null);

        RootNode<Object[]> node = new RootNode<>(ectx, rowType);

        node.register(inbox);

        while (node.hasNext()) {
            Object[] row = node.next();

            assertTrue((Integer)row[0] >= 2);
        }
    }
}

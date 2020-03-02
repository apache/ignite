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
import java.util.UUID;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class ExecutionTest extends AbstractExecutionTest {
    /**
     * @throws Exception If failed.
     */
    @Before
    @Override public void setup() throws Exception {
        nodesCount = 1;
        super.setup();
    }

    @Test
    public void testSimpleExecution() throws Exception {
        // SELECT P.ID, P.NAME, PR.NAME AS PROJECT
        // FROM PERSON P
        // INNER JOIN PROJECT PR
        // ON P.ID = PR.RESP_ID
        // WHERE P.ID >= 2

        ExecutionContext ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);

        ScanNode persons = new ScanNode(ctx, Arrays.asList(
            new Object[]{0, "Igor", "Seliverstov"},
            new Object[]{1, "Roman", "Kondakov"},
            new Object[]{2, "Ivan", "Pavlukhin"},
            new Object[]{3, "Alexey", "Goncharuk"}
        ));

        ScanNode projects = new ScanNode(ctx, Arrays.asList(
            new Object[]{0, 2, "Calcite"},
            new Object[]{1, 1, "SQL"},
            new Object[]{2, 2, "Ignite"},
            new Object[]{3, 0, "Core"}
        ));

        JoinNode join = new JoinNode(ctx, r -> r[0] == r[4]);
        join.register(F.asList(persons, projects));

        ProjectNode project = new ProjectNode(ctx, r -> new Object[]{r[0], r[1], r[5]});
        project.register(join);

        FilterNode filter = new FilterNode(ctx, r -> (Integer) r[0] >= 2);
        filter.register(project);

        RootNode node = new RootNode(ctx, r -> {});
        node.register(filter);

        assert node.hasNext();

        ArrayList<Object[]> rows = new ArrayList<>();

        while (node.hasNext())
            rows.add(node.next());

        assertEquals(2, rows.size());

        Assert.assertArrayEquals(new Object[]{2, "Ivan", "Calcite"}, rows.get(0));
        Assert.assertArrayEquals(new Object[]{2, "Ivan", "Ignite"}, rows.get(1));
    }
}

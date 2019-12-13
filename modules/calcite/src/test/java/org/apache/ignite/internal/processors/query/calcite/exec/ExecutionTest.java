/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.exec;

import java.util.ArrayList;
import java.util.Arrays;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class ExecutionTest extends GridCommonAbstractTest {
    @Test
    public void testSimpleExecution() {
        // SELECT P.ID, P.NAME, PR.NAME AS PROJECT
        // FROM PERSON P
        // INNER JOIN PROJECT PR
        // ON P.ID = PR.RESP_ID
        // WHERE P.ID >= 2

        ConsumerNode node = new ConsumerNode();

        FilterNode filter = new FilterNode(node.sink(), r -> (Integer) r[0] >= 2);
        node.source(filter);

        ProjectNode project = new ProjectNode(filter.sink(), r -> new Object[]{r[0], r[1], r[5]});
        filter.source(project);

        JoinNode join = new JoinNode(project.sink(), (r1, r2) -> r1[0] != r2[1] ? null : new Object[]{r1[0], r1[1], r1[2], r2[0], r2[1], r2[2]});
        project.source(join);

        ScanNode persons = new ScanNode(join.sink(0), Arrays.asList(
            new Object[]{0, "Igor", "Seliverstov"},
            new Object[]{1, "Roman", "Kondakov"},
            new Object[]{2, "Ivan", "Pavlukhin"},
            new Object[]{3, "Alexey", "Goncharuk"}
        ));

        ScanNode projects = new ScanNode(join.sink(1), Arrays.asList(
            new Object[]{0, 2, "Calcite"},
            new Object[]{1, 1, "SQL"},
            new Object[]{2, 2, "Ignite"},
            new Object[]{3, 0, "Core"}
        ));

        join.sources(Arrays.asList(persons, projects));

        assert node.hasNext();

        ArrayList<Object[]> rows = new ArrayList<>();

        while (node.hasNext()) {
            rows.add(node.next());
        }

        assertEquals(2, rows.size());

        Assert.assertArrayEquals(new Object[]{2, "Ivan", "Calcite"}, rows.get(0));
        Assert.assertArrayEquals(new Object[]{2, "Ivan", "Ignite"}, rows.get(1));
    }
}

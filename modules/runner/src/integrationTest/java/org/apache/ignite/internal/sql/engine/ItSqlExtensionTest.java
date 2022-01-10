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

package org.apache.ignite.internal.sql.engine;

import java.util.stream.Collectors;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.sql.engine.extension.TestExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Test cases for SQL Extension API.
 */
@Disabled("https://issues.apache.org/jira/browse/IGNITE-15655")
public class ItSqlExtensionTest extends AbstractBasicIntegrationTest {
    /**
     * Before all.
     */
    @BeforeAll
    static void initTestData() {
        createAndPopulateTable();
    }

    @Test
    public void test() {
        TestExtension.allNodes = CLUSTER_NODES.stream()
                .map(i -> (IgniteImpl) i)
                .map(IgniteImpl::id)
                .collect(Collectors.toList());

        assertQuery(""
                + "select t.node_id,"
                + "       t.num,"
                + "       p.name"
                + "  from person p"
                + "  join TEST_EXT.CUSTOM_SCHEMA.TEST_TBL t "
                + "    on p.id = t.num"
                + " where t.num <> 1")
                .columnNames("NODE_ID", "NUM", "NAME")
                .columnTypes(String.class, Integer.class, String.class)
                .returns(nodeId(0), 2, "Ilya")
                .returns(nodeId(1), 2, "Ilya")
                .returns(nodeId(2), 2, "Ilya")
                .returns(nodeId(0), 3, "Roma")
                .returns(nodeId(1), 3, "Roma")
                .returns(nodeId(2), 3, "Roma")
                .check();
    }

    private String nodeId(int idx) {
        return ((IgniteImpl) CLUSTER_NODES.get(idx)).id();
    }
}

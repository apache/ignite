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

package org.apache.ignite.internal.processors.query.calcite.integration;

import java.sql.Date;
import java.util.Collection;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.calcite.QueryChecker;
import org.apache.ignite.internal.processors.query.calcite.util.IgniteResource;
import org.hamcrest.Matcher;
import org.junit.Test;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_CLIENT_MODE;

/**
 * Integration tests for system views.
 */
public class SystemViewsIntegrationTest extends AbstractBasicIntegrationTest {
    /** */
    @Test
    public void testSimpleView() {
        Collection<ClusterNode> nodes = client.cluster().nodes();

        // Check count of rows.
        assertQuery("SELECT COUNT(*) FROM sys.nodes").returns((long)nodes.size()).check();

        // Check simple query.
        QueryChecker checker = assertQuery("SELECT node_id, consistent_id, node_order FROM sys.nodes");

        for (ClusterNode node : nodes)
            checker.returns(node.id(), node.consistentId().toString(), node.order());

        checker.check();

        // Check date type.
        assertQuery("SELECT node_start_time FROM sys.node_metrics WHERE node_id = ?")
            .withParams(client.localNode().id())
            .returns(new Date(client.localNode().metrics().getNodeStartTime()));
    }

    /** */
    @Test
    public void testMetricsView() {
        assertQuery("SELECT value FROM sys.metrics WHERE name = 'cluster.TotalClientNodes'").returns("1").check();
    }

    /** */
    @Test
    public void testFiltrableView() {
        Matcher<String> containsIdx = QueryChecker.containsIndexScan("SYS", "NODE_ATTRIBUTES", "NODE_ATTRIBUTES_IDX");

        // Check filtered system view with all of indexed fields set.
        assertQuery("SELECT node_id, name, value FROM sys.node_attributes WHERE node_id = ? AND name = ?")
            .matches(containsIdx)
            .withParams(client.localNode().id(), ATTR_CLIENT_MODE)
            .returns(client.localNode().id(), ATTR_CLIENT_MODE, "true")
            .check();

        // Check filtered system view with any of indexed fields set.
        assertQuery("SELECT node_id, name, VALUE FROM sys.node_attributes WHERE node_id = ? AND value = 'true'")
            .withParams(client.localNode().id())
            .matches(containsIdx)
            .check();

        // Check filtered system view with non-equality operator on indexed field.
        assertQuery("SELECT node_id, name, value FROM sys.node_attributes WHERE node_id = ? AND name >= ? AND name <= ?")
            .matches(containsIdx)
            .withParams(client.localNode().id(), ATTR_CLIENT_MODE, ATTR_CLIENT_MODE)
            .returns(client.localNode().id(), ATTR_CLIENT_MODE, "true")
            .check();

        // Check that index don't provide collation for filtered columns.
        Matcher<String> containsSort = QueryChecker.containsSubPlan("IgniteSort");

        assertQuery("SELECT * FROM sys.node_attributes ORDER BY node_id").matches(containsSort).check();
        assertQuery("SELECT * FROM sys.node_attributes ORDER BY name").matches(containsSort).check();
    }

    /** */
    @Test
    public void testJoin() {
        // Join view with view.
        assertQuery("SELECT n.node_id, n.node_order, na.name, na.value " +
            "FROM sys.nodes n JOIN sys.node_attributes na ON N.node_id = na.node_id " +
            "WHERE n.node_id = ? AND na.name = ?")
            .matches(QueryChecker.containsIndexScan("SYS", "NODE_ATTRIBUTES", "NODE_ATTRIBUTES_IDX"))
            .withParams(client.localNode().id(), ATTR_CLIENT_MODE)
            .returns(client.localNode().id(), client.localNode().order(), ATTR_CLIENT_MODE, "true")
            .check();

        // Join view with regular table.
        sql("CREATE TABLE t(a bigint, b varchar)");
        sql("INSERT INTO t VALUES (?, 'test')", client.localNode().order());

        assertQuery("SELECT node_id, node_order, b FROM sys.nodes JOIN t ON node_order = a")
            .returns(client.localNode().id(), client.localNode().order(), "test")
            .check();
    }

    /** */
    @Test
    public void testViewAsSourceForDml() {
        sql("CREATE TABLE t(a bigint)");
        sql("INSERT INTO t SELECT node_order FROM sys.nodes WHERE node_id = ?", client.localNode().id());
        assertQuery("SELECT * FROM t").returns(client.localNode().order()).check();

        sql("MERGE INTO t USING sys.nodes ON node_order = a " +
            "WHEN MATCHED THEN UPDATE SET a = -node_order " +
            "WHEN NOT MATCHED THEN INSERT VALUES node_order");

        assertQuery("SELECT COUNT(*) FROM t").returns((long)client.cluster().nodes().size()).check();
    }

    /** */
    @Test
    public void testDdlOnView() {
        String msg = "DDL statements are not supported on SYS schema";

        assertThrows("CREATE TABLE sys.nodes (a INT)", IgniteSQLException.class, msg);
        assertThrows("ALTER TABLE sys.nodes ADD COLUMN a INT", IgniteSQLException.class, msg);
        assertThrows("ALTER TABLE sys.nodes DROP COLUMN node_order", IgniteSQLException.class, msg);
        assertThrows("ALTER TABLE sys.nodes NOLOGGING", IgniteSQLException.class, msg);
        assertThrows("DROP TABLE sys.nodes", IgniteSQLException.class, msg);
        assertThrows("CREATE INDEX my_idx ON sys.nodes (node_order)", IgniteSQLException.class, msg);
        assertThrows("DROP INDEX sys.node_attributes_idx", IgniteSQLException.class, msg);
    }

    /** */
    @Test
    public void testDmlOnView() {
        String msg = IgniteResource.INSTANCE.modifyTableNotSupported("SYS.NODES").str();

        assertThrows("UPDATE sys.nodes SET node_order = 1", IgniteSQLException.class, msg);
        assertThrows("INSERT INTO sys.nodes (node_order) VALUES (1)", IgniteSQLException.class, msg);
        assertThrows("DELETE FROM sys.nodes", IgniteSQLException.class, msg);
        assertThrows("MERGE INTO sys.nodes AS dst USING (VALUES (1)) AS v(a) ON dst.node_order = v.a " +
            "WHEN MATCHED THEN UPDATE SET node_order = 1", IgniteSQLException.class, msg);
    }
}

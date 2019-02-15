/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.sqltests;

import java.util.Arrays;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Includes all base sql test plus tests that make sense in partitioned mode.
 */
@RunWith(JUnit4.class)
public class PartitionedSqlTest extends BaseSqlTest {
    /** {@inheritDoc} */
    @Override protected void setupData() {
        super.createTables("template=partitioned");

        fillCommonData();
    }

    /**
     * Check distributed INNER JOIN.
     */
    @Test
    public void testInnerDistributedJoin() {
        Arrays.asList(true, false).forEach(forceOrder -> testAllNodes(node -> {
            final String qryTpl = "SELECT d.id, d.name, a.address " +
                "FROM Department d INNER JOIN Address a " +
                "ON d.%s = a.%s";

            Result actIdxOnOn = executeFrom(distributedJoinQry(forceOrder, qryTpl, "id", "depId"), node);
            Result actIdxOffOn = executeFrom(distributedJoinQry(true, qryTpl, "idNoidx", "depId"), node);

            List<List<Object>> exp = doInnerJoin(node.cache(DEP_CACHE_NAME), node.cache(ADDR_CACHE_NAME),
                (dep, addr) -> sqlEq(dep.get("ID"), addr.get("DEPID")),
                (dep, addr) -> Arrays.asList(dep.get("ID"), dep.get("NAME"), addr.get("ADDRESS")));

            assertContainsEq("Distributed join on 'idx = idx' returned unexpected result. " +
                "Preserve join order = " + forceOrder + ".", actIdxOnOn.values(), exp);
            assertContainsEq("Distributed join on 'noidx = idx' returned unexpected result. " +
                "Preserve join order = " + forceOrder + ".", actIdxOffOn.values(), exp);
        }));
    }

    /**
     * Check that if required index is missed, correct exception will be thrown.
     */
    @Test
    public void testInnerDistJoinMissedIndex() {
        Arrays.asList(true, false).forEach(forceOrder -> testAllNodes(node -> {
            String qryTpl = "SELECT d.id, d.name, a.address " +
                "FROM Department d INNER JOIN Address a " +
                "ON d.%s = a.%s";

            assertDistJoinHasIncorrectIndex(
                () -> executeFrom(distributedJoinQry(forceOrder, qryTpl, "idNoidx", "depIdNoidx"), node));

            assertDistJoinHasIncorrectIndex(
                () -> executeFrom(distributedJoinQry(true, qryTpl, "id", "depIdNoidx"), node));
        }));
    }

    /**
     * Check distributed LEFT JOIN.
     */
    @Test
    public void testLeftDistributedJoin() {
        Arrays.asList(true, false).forEach(forceOrder -> testAllNodes(node -> {
            final String qryTpl = "SELECT d.id, d.name, a.depId, a.address " +
                "FROM Department d LEFT JOIN Address a " +
                "ON d.%s = a.%s";

            Result actIdxOnOn = executeFrom(distributedJoinQry(forceOrder, qryTpl, "id", "depId"), node);
            Result actIdxOffOn = executeFrom(distributedJoinQry(true, qryTpl, "idNoidx", "depId"), node);

            List<List<Object>> exp = doLeftJoin(node.cache(DEP_CACHE_NAME), node.cache(ADDR_CACHE_NAME),
                (dep, addr) -> sqlEq(dep.get("ID"), addr.get("DEPID")),
                (dep, addr) -> Arrays.asList(dep.get("ID"), dep.get("NAME"), addr.get("DEPID"), addr.get("ADDRESS")));

            assertContainsEq("Distributed join on 'idx = idx' returned unexpected result. " +
                "Preserve join order = " + forceOrder + ".", actIdxOnOn.values(), exp);
            assertContainsEq("Distributed join on 'noidx = idx' returned unexpected result. " +
                "Preserve join order = " + forceOrder + ".", actIdxOffOn.values(), exp);
        }));
    }

    /**
     * Check that if required index is missed, correct exception will be thrown.
     */
    @Test
    public void testLeftDistributedJoinMissedIndex() {
        Arrays.asList(true, false).forEach(forceOrder -> testAllNodes(node -> {
            String qryTpl = "SELECT d.id, d.name, a.address " +
                "FROM Department d LEFT JOIN Address a " +
                "ON d.%s = a.%s";

            assertDistJoinHasIncorrectIndex(
                () -> executeFrom(distributedJoinQry(forceOrder, qryTpl, "id", "depIdNoidx"), node));

            assertDistJoinHasIncorrectIndex(
                () -> executeFrom(distributedJoinQry(true, qryTpl, "idNoIdx", "depIdNoidx"), node));
        }));
    }

    /**
     * Check distributed RIGHT JOIN.
     */
    @Test
    public void testRightDistributedJoin() {
        setExplain(true);

        Arrays.asList(true, false).forEach(forceOrder -> testAllNodes(node -> {
            final String qryTpl = "SELECT d.id, d.name, a.address " +
                "FROM Department d RIGHT JOIN Address a " +
                "ON d.%s = a.%s";

            Result actIdxOnOn = executeFrom(distributedJoinQry(forceOrder, qryTpl, "id", "depId"), node);
            Result actIdxOnOff = executeFrom(distributedJoinQry(true, qryTpl, "id", "depIdNoidx"), node);

            List<List<Object>> exp = doRightJoin(node.cache(DEP_CACHE_NAME), node.cache(ADDR_CACHE_NAME),
                (dep, addr) -> sqlEq(dep.get("ID"), addr.get("DEPID")),
                (dep, addr) -> Arrays.asList(dep.get("ID"), dep.get("NAME"), addr.get("ADDRESS")));

            assertContainsEq("Distributed join on 'idx = idx' returned unexpected result. " +
                "Preserve join order = " + forceOrder + ".", actIdxOnOn.values(), exp);

            assertContainsEq("Distributed join on 'idx = noidx' returned unexpected result. " +
                "Preserve join order = " + forceOrder + ".", actIdxOnOff.values(), exp);
        }));
    }

    /**
     * Check that if required index is missed, correct exception will be thrown.
     */
    @Test
    public void testRightDistributedJoinMissedIndex() {
        Arrays.asList(true, false).forEach(forceOrder -> testAllNodes(node -> {
            String qryTpl = "SELECT d.id, d.name, a.address " +
                "FROM Department d RIGHT JOIN Address a " +
                "ON d.%s = a.%s";

            assertDistJoinHasIncorrectIndex(
                () -> executeFrom(distributedJoinQry(forceOrder, qryTpl, "idNoidx", "depIdNoidx"), node));

            assertDistJoinHasIncorrectIndex(
                () -> executeFrom(distributedJoinQry(true, qryTpl, "idNoidx", "depId"), node));
        }));
    }
}

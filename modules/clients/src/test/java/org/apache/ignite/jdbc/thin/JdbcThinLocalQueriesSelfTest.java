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

package org.apache.ignite.jdbc.thin;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test that replicated-only query is executed locally.
 */
@RunWith(JUnit4.class)
public class JdbcThinLocalQueriesSelfTest extends JdbcThinAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrid(0);

        startGrid(1);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     *
     */
    @Test
    public void testLocalThinJdbcQuery() throws SQLException {
        try (Connection c = connect(grid(0), "replicatedOnly=true")) {
            execute(c, "CREATE TABLE Company(id int primary key, name varchar) WITH " +
                "\"template=replicated,cache_name=Company\"");

            execute(c, "CREATE TABLE Person(id int primary key, name varchar, companyid int) WITH " +
                "\"template=replicated,cache_name=Person\"");

            execute(c, "insert into Company(id, name) values (1, 'Apple')");

            execute(c, "insert into Person(id, name, companyid) values (2, 'John', 1)");

            List<List<?>> res = execute(c, "SELECT p.id, p.name, c.name from Person p left join Company c on " +
                "p.companyid = c.id");

            assertEqualsCollections(F.asList(2, "John", "Apple"), res.get(0));

            Map twoStepCache = U.field(grid(0).context().query().getIndexing(), "twoStepCache");

            // No two step queries cached => local select.
            assertEquals(0, twoStepCache.size());
        }
    }
}

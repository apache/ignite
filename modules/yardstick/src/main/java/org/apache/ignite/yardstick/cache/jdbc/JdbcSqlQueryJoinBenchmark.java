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

package org.apache.ignite.yardstick.cache.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.yardstickframework.BenchmarkConfiguration;

import static org.apache.ignite.yardstick.IgniteAbstractBenchmark.nextRandom;
import static org.yardstickframework.BenchmarkUtils.println;

/**
 * JDBC benchmark that performs query operations w/joins
 */
public class JdbcSqlQueryJoinBenchmark extends JdbcAbstractBenchmark {
    /** Join query template constant */
    private static final String JOIN_QUERY =
        "select p.id, p.org_id, p.first_name, p.last_name, p.salary, o.name " +
            "from PERSON p " +
            "left join ORGANIZATION o " +
            "on p.id = o.id " +
            "where salary >= ? and salary <= ?";

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        println(cfg, "Populating query data...");

        long start = System.nanoTime();
        final int orgRange = args.range() / 10;

        try (PreparedStatement stmt = conn.get().prepareStatement("insert into ORGANIZATION(id, name) values(?, ?)")) {
            // Populate organizations.
            for (int i = 0; i < orgRange && !Thread.currentThread().isInterrupted(); i++) {
                stmt.setInt(1, i);
                stmt.setString(2, "org" + i);
                stmt.addBatch();
            }
            stmt.executeBatch();
        }

        try (PreparedStatement stmt = conn.get().prepareStatement("insert into PERSON(id, org_id, first_name, last_name," +
            " salary) values(?, ?, ?, ?, ?)")) {
            // Populate persons.
            for (int i = orgRange; i < orgRange + args.range() && !Thread.currentThread().isInterrupted(); i++) {
                stmt.setInt(1, i);
                stmt.setInt(2, nextRandom(orgRange));
                stmt.setString(3, "firstName" + i);
                stmt.setString(4, "lastName" + i);
                stmt.setDouble(5, (i - orgRange) * 1000);
                stmt.addBatch();

                if (i % 100000 == 0)
                    println(cfg, "Populated persons: " + i);
            }
            stmt.executeBatch();
        }

        println(cfg, "Finished populating join query data in " + ((System.nanoTime() - start) / 1_000_000) + " ms.");
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        double salary = ThreadLocalRandom.current().nextDouble() * args.range() * 1000;

        double maxSalary = salary + 1000;

        try (PreparedStatement stmt = conn.get().prepareStatement(JOIN_QUERY)) {
            stmt.setDouble(1, salary);
            stmt.setDouble(2, maxSalary);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    double actualSalary = rs.getDouble(5);

                    if (actualSalary < salary || actualSalary > maxSalary)
                        throw new Exception("Invalid person retrieved [min=" + salary + ", max=" + maxSalary +
                            ", salary=" + actualSalary + ", id=" + rs.getInt(1) + ']');
                }
            }
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public void tearDown() throws Exception {
        if (!args.createTempDatabase()) {
            clearTable("PERSON");
            clearTable("ORGANIZATION");
        }
        super.tearDown();
    }
}

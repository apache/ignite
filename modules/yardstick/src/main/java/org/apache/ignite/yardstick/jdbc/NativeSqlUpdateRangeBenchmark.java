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

package org.apache.ignite.yardstick.jdbc;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.yardstickframework.BenchmarkConfiguration;

/**
 * Native sql benchmark that performs update operations.
 */
public class NativeSqlUpdateRangeBenchmark extends AbstractNativeBenchmark {
    /** Generates disjoint (among threads) id ranges */
    private DisjointRangeGenerator idGen;

    /** Setup method */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);
        idGen = new DisjointRangeGenerator(cfg.threads(), args.range(), args.sqlRange());
    }

    /**
     * Benchmarked action that performs updates (single row or batch).
     *
     * {@inheritDoc}
     */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        long startId = idGen.nextRangeStartId();
        long endId = idGen.endRangeId(startId);

        long expRsSize = idGen.rangeWidth();

        SqlFieldsQuery qry;

        if (idGen.rangeWidth() == 1) {
            qry = new SqlFieldsQuery("UPDATE test_long SET val = (val + 1) WHERE id = ?");

            // startId == endId
            qry.setArgs(startId);
        }
        else {
            qry = new SqlFieldsQuery("UPDATE test_long SET val = (val + 1) WHERE id BETWEEN ? AND ?");

            qry.setArgs(startId, endId);
        }

        try (FieldsQueryCursor<List<?>> cursor = ((IgniteEx)ignite()).context().query()
                .querySqlFields(qry, false)) {
            Iterator<List<?>> it = cursor.iterator();

            List<?> cntRow = it.next();

            long rsSize = (Long)cntRow.get(0);

            if (it.hasNext())
                throw new Exception("Only one row expected on UPDATE query");

            if (rsSize != expRsSize)
                throw new Exception("Invalid result set size [actual=" + rsSize + ", expected=" + expRsSize + ']');
        }

        return true;
    }
}

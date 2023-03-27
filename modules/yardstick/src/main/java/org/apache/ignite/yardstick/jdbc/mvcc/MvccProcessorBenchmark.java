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

package org.apache.ignite.yardstick.jdbc.mvcc;

import java.util.Map;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.yardstick.jdbc.DisjointRangeGenerator;
import org.yardstickframework.BenchmarkConfiguration;

/**
 * Benchmark app that creates load on Mvcc Processor.
 * Should be run in many threads on many hosts against one single server node.
 */
public class MvccProcessorBenchmark extends AbstractDistributedMvccBenchmark {
    /** Generates id, that are disjoint only among threads running current host. */
    private DisjointRangeGenerator locIdGen;

    /** Offset for current host ids range, to make it disjoint among all the other host id ranges. */
    private int idOffset;

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        int locIdRangeWidth = args.range() / driversNodesCnt;

        locIdGen = new DisjointRangeGenerator(cfg.threads(), locIdRangeWidth, args.sqlRange());

        idOffset = locIdRangeWidth * memberId;
    }

    /**
     *  Performs sql updates on the key sets that are disjoint among all the threads on all the hosts.
     */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        long locStart = locIdGen.nextRangeStartId();

        long start = idOffset + locStart;

        long end = idOffset + locIdGen.endRangeId(locStart);

        execute(new SqlFieldsQuery(UPDATE_QRY).setArgs(start, end));

        return true;
    }
}

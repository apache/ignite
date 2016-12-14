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

package org.apache.ignite.internal.util;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.ignite.IgniteLogger;
import org.jsr166.LongAdder8;

/**
 *
 */
public class InternalStatistics {
    /** */
    private final CacheStatistics cache;

    /** */
    private final ConcurrentMap<Class<?>, StatAggregate> stats = new ConcurrentHashMap<>();

    public InternalStatistics() {
        cache = new CacheStatistics(this);
    }

    public void dump(IgniteLogger log) {
        for (StatAggregate stat : stats.values())
            stat.dump(log);
    }

    public void add(OperationStatistic stat) {
        StatAggregate agg = stats.get(stat.getClass());

        if (agg == null) {
            StatAggregate old = stats.putIfAbsent(stat.getClass(), agg = new StatAggregate(stat));

            if (old != null)
                agg = old;
        }

        agg.add(stat);
    }

    public CacheStatistics cache() {
        return cache;
    }

    /**
     *
     */
    static class StatAggregate {
        /** */
        private final LongAdder8 totalTime = new LongAdder8();

        /** */
        private final LongAdder8 cnt = new LongAdder8();

        /** */
        private final LongAdder8[] times;

        /** */
        private final OperationStatistic op;

        StatAggregate(OperationStatistic op) {
            this.op = op;

            this.times = new LongAdder8[op.time().length];

            for (int i = 0; i < times.length; i++)
                times[i] = new LongAdder8();
        }

        void dump(IgniteLogger log) {
            long cnt = this.cnt.sumThenReset();
            long totalTime = this.totalTime.sumThenReset();

            if (cnt > 0) {
                double totAvg = (totalTime / (double)cnt);

                log.info(String.format("Total statistic [name=%s, cnt=%d, avg=%.2f]",
                    op.getClass().getSimpleName(),
                    cnt,
                    totAvg));

                for (int i = 0; i < times.length; i++) {
                    long time = times[i].sumThenReset();

                    if (time > 0) {
                        double avg = (time / (double)cnt);

                        log.info(String.format("Part statistic [name=%s, cnt=%d, avg=%.2f, p=%.2f]",
                            op.opName(i),
                            cnt,
                            avg,
                            avg / totAvg));
                    }
                }
            }
            else {
                for (int i = 0; i < times.length; i++)
                    times[i].sumThenReset();
            }
        }

        public void add(OperationStatistic op) {
            if (op.endTime() > 0) {
                cnt.increment();

                totalTime.add(op.endTime() - op.startTime());

                long time[] = op.time();

                for (int i = 0; i < time.length; i++) {
                    long time0 = time[i];

                    if (time0 > 0)
                        times[i].add(time0);
                }
            }
        }
    }
}

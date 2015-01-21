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

package org.gridgain.loadtests.streamer.average;

import org.apache.ignite.*;
import org.apache.ignite.streamer.*;
import org.gridgain.grid.util.typedef.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Stage for average benchmark.
 */
class TestStage implements StreamerStage<Integer> {
    /** {@inheritDoc} */
    @Override public String name() {
        return "stage";
    }

    /** {@inheritDoc} */
    @Override public Map<String, Collection<?>> run(StreamerContext ctx, Collection<Integer> evts)
        throws IgniteCheckedException {
        ConcurrentMap<String, TestAverage> loc = ctx.localSpace();

        TestAverage avg = loc.get("avg");

        if (avg == null)
            avg = F.addIfAbsent(loc, "avg", new TestAverage());

        for (Integer e : evts)
            avg.increment(e, 1);

        StreamerWindow<Integer> win = ctx.window();

        win.enqueueAll(evts);

        while (true) {
            Integer e = win.pollEvicted();

            if (e == null)
                break;

            // Subtract evicted events from running total.
            avg.increment(-e, -1);
        }

        return null;
    }
}

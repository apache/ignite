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

import java.util.Arrays;

/**
 *
 */
public class OperationStatistic {
    /** */
    private long startTime;

    /** */
    private long endTime;

    /** */
    private final long[] time;

    OperationStatistic(int ops) {
        time = new long[ops];
    }

    public final long[] time() {
        return time;
    }

    public final long startTime() {
        return startTime;
    }

    public final long endTime() {
        return endTime;
    }

    public String opName(int op) {
        return "N/A";
    }

    public final void start() {
        startTime = System.nanoTime();

        Arrays.fill(time, -1L);
    }

    public final void end() {
        endTime = System.nanoTime();
    }

    public final void startOp(Enum op) {
        startOp(op.ordinal());
    }

    public final void endOp(Enum op) {
        endOp(op.ordinal());
    }

    public final void startOp(int op) {
        assert time[op] == -1L : time[op];

        time[op] = System.nanoTime();
    }

    public final void endOp(int op) {
        long start = time[op];

        assert start > 0 : start;

        time[op] = System.nanoTime() - start;
    }
}

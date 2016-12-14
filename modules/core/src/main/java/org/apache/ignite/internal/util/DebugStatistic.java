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

import org.jsr166.LongAdder8;

/**
 *
 */
public class DebugStatistic {
    /** */
    private final String name;

    /** */
    private final LongAdder8 time = new LongAdder8();

    /** */
    private final LongAdder8 cnt = new LongAdder8();

    DebugStatistic(String name) {
        this.name = name;
    }

    public long start() {
        return System.nanoTime();
    }

    public String name() {
        return name;
    }

    public void addTime(long start) {
        time.add(System.nanoTime() - start);

        cnt.increment();
    }

    public LongAdder8 time() {
        return time;
    }

    public LongAdder8 count() {
        return cnt;
    }
}

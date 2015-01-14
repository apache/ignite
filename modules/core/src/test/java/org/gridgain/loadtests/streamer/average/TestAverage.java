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

/**
 * Average helper class.
 */
class TestAverage {
    /** */
    private int total;

    /** */
    private int cnt;

    /**
     * @param avg Average.
     */
    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    public void increment(TestAverage avg) {
        int total;
        int cnt;

        synchronized (avg) {
            total = avg.total;
            cnt = avg.cnt;
        }

        increment(total, cnt);
    }

    /**
     * @param total Increment total.
     * @param cnt Increment count.
     */
    public synchronized void increment(int total, int cnt) {
        this.total += total;
        this.cnt += cnt;
    }

    /**
     * @param total Total.
     * @param cnt Count.
     */
    public synchronized void set(int total, int cnt) {
        this.total = total;
        this.cnt = cnt;
    }

    /**
     * @return Running average.
     */
    public synchronized double average() {
        return (double)total / cnt;
    }
}

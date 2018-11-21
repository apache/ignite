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
 *
 */

package org.apache.ignite.internal.processors.cache;

/**
 */
public class UpdateCounterGenerator {
    /** Low watermark. */
    private long lwm;

    /** High watermark. */
    private long hwm;

    /** Counts distance until gap is closed. */
    private long cnt;

    public UpdateCounterGenerator(long lwm, long hwm, long cnt) {
        this.lwm = lwm;
        this.hwm = hwm;
        this.cnt = cnt;
    }

    public static void main(String[] args) {
        UpdateCounterGenerator gd = new UpdateCounterGenerator(0, 0, 0);
        System.out.println(gd);
        gd.update(2);
        System.out.println(gd);
        gd.update(3);
        System.out.println(gd);
        gd.update(6);
        System.out.println(gd);
        gd.update(4);
        System.out.println(gd);
        gd.update(5);
        System.out.println(gd);
        gd.update(7);
        System.out.println(gd);
        gd.update(10);
        System.out.println(gd);
        gd.update(8);
        System.out.println(gd);
        gd.update(9);
        System.out.println(gd);
        gd.update(15);
        System.out.println(gd);
    }

    /**
     * @param val Value.
     */
    public void update(long val) {
        cnt++;

        if (val > hwm)
            hwm = val;

        if (lwm + cnt == hwm) {
            lwm = hwm;

            cnt = 0;
        }
        else if (lwm + 1 == val) {
            lwm++;

            cnt--;
        }
    }

    public long lwm() {
        return lwm;
    }

    public long gap() {
        return hwm - lwm;
    }

    @Override public String toString() {
        return "GapsDetector{" +
            "lwm=" + lwm +
            ", gap=" + gap() +
            '}';
    }

    public long hwm() {
        return hwm;
    }

    public long gapCount() {
        return cnt;
    }
}

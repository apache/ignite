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

package org.apache.ignite.internal.processors.cache.persistence;

/**
 * Detect gaps in growing integer sequence.
 */
public class GapsDetector {
    private int lwm = -1;
    private int hwm = -1;
    private int cnt = 0;

    public GapsDetector(int start) {
        lwm = hwm = start;
    }

    public static void main(String[] args) {
        GapsDetector gd = new GapsDetector(1);
        gd.add(3);

        for (int i = 1; i < 20; i++) {
            gd.add(3 + i * 2);
            gd.add(2 + i * 2);

            System.out.println(gd.getLwm());
        }

        gd.add(2);

        System.out.println(gd.hasGaps() + " " + gd.getLwm());
    }

    /**
     * @param val Value.
     */
    public void add(int val) {
        System.out.println(val);

        cnt++;

        if (val > hwm)
            hwm = val;

        if (lwm + cnt == hwm) {
            lwm = hwm;

            cnt = 0;
        }
    }

    public int getLwm() {
        return lwm;
    }

    /**
     * @return {@Code true} if gaps are in sequence.
     */
    public boolean hasGaps() {
        return hwm - lwm > 0;
    }
}

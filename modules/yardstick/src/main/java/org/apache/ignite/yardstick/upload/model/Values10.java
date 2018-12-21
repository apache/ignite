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

package org.apache.ignite.yardstick.upload.model;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Describes value_type for data model, defined in {@link QueryFactory#createTable()}.
 */
public class Values10 {
    /** */
    final String val1;

    /** */
    final long val2;

    /** */
    final String val3;

    /** */
    final long val4;

    /** */
    final String val5;

    /** */
    final long val6;

    /** */
    final String val7;

    /** */
    final long val8;

    /** */
    final String val9;

    /** */
    final long val10;

    /** Creates new object with randomly initialized fields */
    public Values10() {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        val1 = String.valueOf(rnd.nextLong());
        val2 = rnd.nextLong();

        val3 = String.valueOf(rnd.nextLong());
        val4 = rnd.nextLong();

        val5 = String.valueOf(rnd.nextLong());
        val6 = rnd.nextLong();

        val7 = String.valueOf(rnd.nextLong());
        val8 = rnd.nextLong();

        val9 = String.valueOf(rnd.nextLong());
        val10 = rnd.nextLong();
    }

    public Object[] toArgs(long id) {
        return new Object[] {id, val1, val2, val3, val4, val5, val6, val7, val8, val9, val10};
    }

    /**
     * @param valIdx index of field (value).
     * @return name of the field.
     */
    public static String fieldName(int valIdx) {
        if (valIdx > 10 || valIdx < 1)
            throw new IllegalArgumentException("Incorrect value index [" + valIdx + "]." +
                " Value index should be in range [1..10].");

        return "val" + valIdx;
    }
}

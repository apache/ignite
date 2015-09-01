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

package org.apache.ignite.yardstick.cache.model;

import java.io.Serializable;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

/**
 * Value used for indexed put test.
 */
public class Person8 implements Serializable {
    /** Value 1. */
    @QuerySqlField(index = true)
    private int val1;

    /** Value 2. */
    @QuerySqlField(index = true)
    private int val2;

    /** Value 3. */
    @QuerySqlField(index = true)
    private int val3;

    /** Value 4. */
    @QuerySqlField(index = true)
    private int val4;

    /** Value 5. */
    @QuerySqlField(index = true)
    private int val5;

    /** Value 6. */
    @QuerySqlField(index = true)
    private int val6;

    /** Value 7. */
    @QuerySqlField(index = true)
    private int val7;

    /** Value 8. */
    @QuerySqlField(index = true)
    private int val8;

    /**
     * Constructs.
     *
     * @param val Indexed value.
     */
    public Person8(int val) {
        this.val1 = val;
        this.val2 = val + 1;
        this.val3 = val + 2;
        this.val4 = val + 3;
        this.val5 = val + 4;
        this.val6 = val + 5;
        this.val7 = val + 6;
        this.val8 = val + 7;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        Person8 p8 = (Person8)o;

        return val1 == p8.val1 && val2 == p8.val2 && val3 == p8.val3 && val4 == p8.val4
            && val5 == p8.val5 && val6 == p8.val6 && val7 == p8.val7 && val8 == p8.val8;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = val1;

        result = 31 * result + val2;
        result = 31 * result + val3;
        result = 31 * result + val4;
        result = 31 * result + val5;
        result = 31 * result + val6;
        result = 31 * result + val7;
        result = 31 * result + val8;

        return result;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "Person8 [val1=" + val1 + ", val2=" + val2 + ", val3=" + val3 + ", val4=" + val4 + ", val5=" + val5 +
            ", val6=" + val6 + ", val7=" + val7 + ", val8=" + val8 +']';
    }
}
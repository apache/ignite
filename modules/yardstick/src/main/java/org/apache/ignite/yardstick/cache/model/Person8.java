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

import org.apache.ignite.cache.query.annotations.*;

import java.io.*;

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
     * Empty constructor.
     */
    public Person8() {
        // No-op.
    }

    /**
     * Constructs.
     */
    public Person8(int val1, int val2, int val3, int val4, int val5, int val6, int val7, int val8) {
        this.val1 = val1;
        this.val2 = val2;
        this.val3 = val3;
        this.val4 = val4;
        this.val5 = val5;
        this.val6 = val6;
        this.val7 = val7;
        this.val8 = val8;
    }

    /**
     * @return Get value.
     */
    public int getVal1() {
        return val1;
    }

    /**
     * Set value.
     *
     * @param val1 Value.
     */
    public void setVal1(int val1) {
        this.val1 = val1;
    }

    /**
     * @return Get value.
     */
    public int getVal2() {
        return val2;
    }

    /**
     * Set value.
     *
     * @param val2 Value.
     */
    public void setVal2(int val2) {
        this.val2 = val2;
    }

    /**
     * @return Get value.
     */
    public int getVal3() {
        return val3;
    }

    /**
     * Set value.
     *
     * @param val3 Value.
     */
    public void setVal3(int val3) {
        this.val3 = val3;
    }

    /**
     * @return Get value.
     */
    public int getVal4() {
        return val4;
    }

    /**
     * Set value.
     *
     * @param val4 Value.
     */
    public void setVal4(int val4) {
        this.val4 = val4;
    }

    /**
     * @return Get value.
     */
    public int getVal5() {
        return val5;
    }

    /**
     * Set value.
     *
     * @param val5 Value.
     */
    public void setVal5(int val5) {
        this.val5 = val5;
    }

    /**
     * @return Get value.
     */
    public int getVal6() {
        return val6;
    }

    /**
     * Set value.
     *
     * @param val6 Value.
     */
    public void setVal6(int val6) {
        this.val6 = val6;
    }

    /**
     * @return Get value.
     */
    public int getVal7() {
        return val7;
    }

    /**
     * Set value.
     *
     * @param val7 Value.
     */
    public void setVal7(int val7) {
        this.val7 = val7;
    }

    /**
     * @return Get value.
     */
    public int getVal8() {
        return val8;
    }

    /**
     * Set value.
     *
     * @param val8 Value.
     */
    public void setVal8(int val8) {
        this.val8 = val8;
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

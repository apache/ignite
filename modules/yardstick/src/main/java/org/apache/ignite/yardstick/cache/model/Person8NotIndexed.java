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

/**
 * Value used for create index test.
 */
public class Person8NotIndexed implements Serializable {
    /** Value 1. */
    private int val1;

    /** Value 2. */
    private int val2;

    /** Value 3. */
    private int val3;

    /** Value 4. */
    private int val4;

    /** Value 5. */
    private int val5;

    /** Value 6. */
    private int val6;

    /** Value 7. */
    private int val7;

    /** Value 8. */
    private int val8;

    /** */
    public Person8NotIndexed() {
    }

    /**
     * Constructs.
     *
     * @param val Indexed value.
     */
    public Person8NotIndexed(int val) {
        this.val1 = val;
        this.val2 = val + 1;
        this.val3 = val + 2;
        this.val4 = val + 3;
        this.val5 = val + 4;
        this.val6 = val + 5;
        this.val7 = val + 6;
        this.val8 = val + 7;
    }

    /**
     * @return val1
     */
    public int getVal1() {
        return val1;
    }

    /**
     * @param val1 val1
     */
    public void setVal1(int val1) {
        this.val1 = val1;
    }

    /**
     * @return val2
     */
    public int getVal2() {
        return val2;
    }

    /**
     * @param val2 val2
     */
    public void setVal2(int val2) {
        this.val2 = val2;
    }

    /**
     * @return val3
     */
    public int getVal3() {
        return val3;
    }

    /**
     * @param val3 val3
     */
    public void setVal3(int val3) {
        this.val3 = val3;
    }

    /**
     * @return val4
     */
    public int getVal4() {
        return val4;
    }

    /**
     * @param val4 val4
     */
    public void setVal4(int val4) {
        this.val4 = val4;
    }

    /**
     * @return val5
     */
    public int getVal5() {
        return val5;
    }

    /**
     * @param val5 val5
     */
    public void setVal5(int val5) {
        this.val5 = val5;
    }

    /**
     * @return val6
     */
    public int getVal6() {
        return val6;
    }

    /**
     * @param val6 val6
     */
    public void setVal6(int val6) {
        this.val6 = val6;
    }

    /**
     * @return val7
     */
    public int getVal7() {
        return val7;
    }

    /**
     * @param val7 val7
     */
    public void setVal7(int val7) {
        this.val7 = val7;
    }

    /**
     * @return val8
     */
    public int getVal8() {
        return val8;
    }

    /**
     * @param val8 val8
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

        Person8NotIndexed p8 = (Person8NotIndexed)o;

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
        return "Person8NotIndexed [val1=" + val1 + ", val2=" + val2 + ", val3=" + val3 + ", val4=" + val4 + ", val5=" +
            val5 + ", val6=" + val6 + ", val7=" + val7 + ", val8=" + val8 + ']';
    }
}

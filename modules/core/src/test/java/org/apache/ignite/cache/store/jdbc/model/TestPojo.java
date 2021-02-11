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

package org.apache.ignite.cache.store.jdbc.model;

import java.io.Serializable;
import java.sql.Date;
import java.util.Objects;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

/**
 * Test JDBC POJO object.
 */
public class TestPojo implements Serializable {
    /** */
    @QuerySqlField
    private String value1;

    /** */
    @QuerySqlField
    private Integer value2;

    /** */
    @QuerySqlField
    private Date value3;

    /**
     * Default constructor.
     */
    public TestPojo() {
        // No-op.
    }

    /** */
    public TestPojo(String value1, int value2, Date value3) {
        this.value1 = value1;

        this.value2 = value2;

        this.value3 = value3;
    }

    /** */
    public String getValue1() {
        return value1;
    }

    /** */
    public void setValue1(String value1) {
        this.value1 = value1;
    }

    /** */
    public Integer getValue2() {
        return value2;
    }

    /** */
    public void setValue2(Integer value2) {
        this.value2 = value2;
    }

    /** */
    public Date getValue3() {
        return value3;
    }

    /** */
    public void setValue3(Date value3) {
        this.value3 = value3;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        TestPojo pogo = (TestPojo)o;

        return Objects.equals(value1, pogo.value1) &&
            Objects.equals(value2, pogo.value2) &&
            Objects.equals(value3, pogo.value3);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {

        return Objects.hash(value1, value2, value3);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "TestPojo{" +
            "value1='" + value1 + '\'' +
            ", value2=" + value2 +
            ", value3=" + value3 +
            '}';
    }
}


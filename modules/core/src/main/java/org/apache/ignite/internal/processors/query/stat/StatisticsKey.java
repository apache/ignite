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
package org.apache.ignite.internal.processors.query.stat;

import java.io.Serializable;
import java.util.Objects;

/**
 * Key to collect statistics by.
 */
public class StatisticsKey implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Object schema. */
    private final String schema;

    /** Object name. */
    private final String obj;

    /**
     * Constructor.
     *
     * @param schema Object schema.
     * @param obj Object name.
     */
    public StatisticsKey(String schema, String obj) {
        assert obj != null;

        this.schema = schema;
        this.obj = obj;
    }

    /**
     * @return Schema name.
     */
    public String schema() {
        return schema;
    }

    /**
     * @return Object name.
     */
    public String obj() {
        return obj;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StatisticsKey statsKey = (StatisticsKey) o;
        return Objects.equals(schema, statsKey.schema) &&
            Objects.equals(obj, statsKey.obj);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(schema, obj);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "StatsKey{" +
            "schema='" + schema + '\'' +
            ", obj='" + obj + '\'' +
            '}';
    }
}

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

package org.apache.ignite.internal.processors.query.stat.messages;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * Key, describing the object of statistics. For example: table with some columns.
 */
public class StatisticsKeyMessage implements Message, Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final short TYPE_CODE = 183;

    /** Object schema. */
    @Order(0)
    String schema;

    /** Object name. */
    @Order(1)
    String obj;

    /** Optional list of columns to collect statistics by. */
    @Order(2)
    List<String> colNames;

    /**
     * Empty constructor.
     */
    public StatisticsKeyMessage() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param schema Schema name.
     * @param obj Object name.
     * @param colNames Column names.
     */
    public StatisticsKeyMessage(String schema, String obj, List<String> colNames) {
        this.schema = schema;
        this.obj = obj;
        this.colNames = colNames;
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

    /**
     * @return Column names.
     */
    public List<String> colNames() {
        return colNames;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(StatisticsKeyMessage.class, this);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StatisticsKeyMessage that = (StatisticsKeyMessage)o;
        return Objects.equals(schema, that.schema) &&
            Objects.equals(obj, that.obj) &&
            Objects.equals(colNames, that.colNames);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(schema, obj, colNames);
    }
}

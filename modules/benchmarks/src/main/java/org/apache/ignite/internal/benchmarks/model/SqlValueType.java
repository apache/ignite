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

package org.apache.ignite.internal.benchmarks.model;

import java.sql.Timestamp;
import java.util.UUID;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

/** Value class of the benchmarks that query their values with SQL; its table has the class's simple name. */
public enum SqlValueType {
    /** {@link Pojo}: one {@code int} field. */
    POJO,

    /** {@link Pojo8}: eight fields of different types. */
    POJO8;

    /** @return Value class. */
    public Class<?> valueClass() {
        return this == POJO ? Pojo.class : Pojo8.class;
    }

    /**
     * @param id Value of the indexed field {@code id}.
     * @return Value.
     */
    public Object value(int id) {
        return this == POJO ? new Pojo(id) : new Pojo8(id);
    }

    /** @return {@code INSERT} of a row, with the arguments of {@link #insertArgs}. */
    public String insertSql() {
        return this == POJO
            ? "INSERT INTO Pojo (_key, id) VALUES (?, ?)"
            : "INSERT INTO Pojo8 (_key, id, longVal, doubleVal, boolVal, str, uuid, ts, bytes) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
    }

    /**
     * @param key Key, also the value of {@code id}.
     * @return Arguments of {@link #insertSql()}.
     */
    public Object[] insertArgs(Integer key) {
        if (this == POJO)
            return new Object[] {key, key};

        Pojo8 v = new Pojo8(key);

        return new Object[] {key, key, v.longVal, v.doubleVal, v.boolVal, v.str, v.uuid, v.ts, v.bytes};
    }

    /** Value with one field. */
    private static class Pojo {
        /** */
        @QuerySqlField(index = true)
        private final int id;

        /** */
        Pojo(int id) {
            this.id = id;
        }
    }

    /** Value with eight fields of different types. */
    private static class Pojo8 {
        /** */
        @QuerySqlField(index = true)
        private final int id;

        /** */
        @QuerySqlField
        private long longVal = 1_000_000_000_000L;

        /** */
        @QuerySqlField
        private double doubleVal = 1.5d;

        /** */
        @QuerySqlField
        private boolean boolVal = true;

        /** */
        @QuerySqlField
        private String str = "abcdefghijklmnop";

        /** */
        @QuerySqlField
        private UUID uuid = new UUID(1L, 2L);

        /** */
        @QuerySqlField
        private Timestamp ts = new Timestamp(1_000_000_000_000L);

        /** */
        @QuerySqlField
        private byte[] bytes = new byte[16];

        /** */
        Pojo8(int id) {
            this.id = id;
        }
    }
}

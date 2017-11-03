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

package org.apache.ignite.spark

import java.lang.Boolean
import java.sql.Timestamp
import java.util.Date

import org.apache.ignite.spark.IgniteRDDSpec.ScalarCacheQuerySqlField

class EntityTestAllTypeFields(
    @ScalarCacheQuerySqlField(index = true) val boolVal: Boolean,
    @ScalarCacheQuerySqlField(index = true) val byteVal: Byte,
    @ScalarCacheQuerySqlField(index = true) val shortVal: Short,
    @ScalarCacheQuerySqlField(index = true) val intVal: Int,
    @ScalarCacheQuerySqlField(index = true) val longVal: Long,
    @ScalarCacheQuerySqlField(index = true) val floatVal: Float,
    @ScalarCacheQuerySqlField(index = true) val doubleVal: Double,
    @ScalarCacheQuerySqlField(index = true) val strVal: String,
    @ScalarCacheQuerySqlField(index = true) val dateVal: Date,
    @ScalarCacheQuerySqlField(index = true) val timestampVal: Timestamp,
    @ScalarCacheQuerySqlField(index = true) val byteArrVal: Array[Byte],
    @ScalarCacheQuerySqlField(index = true) val bigDecVal: java.math.BigDecimal,
    @ScalarCacheQuerySqlField(index = true) val javaSqlDate: java.sql.Date

) extends Serializable {
    def this(
        i: Int
    ) {
        this(
            i % 2 == 0,     // Boolean
            i.toByte,       // Byte
            i.toShort,      // Short
            i,              // Int
            i.toLong,       // Long
            i,              // Float
            i,              // Double
            "name" + i,     // String
            new Date(i),
            new Timestamp(i),
            Array(i.toByte, i.toByte),
            new java.math.BigDecimal(i.toString),
            new java.sql.Date(i))
    }
}

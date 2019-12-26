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

package org.apache.ignite.internal.processors.query.calcite.serialize.type;

import java.io.Serializable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;

/**
 * Serializable RelDataType representation.
 */
public interface DataType extends Serializable {
    /**
     * Factory method to construct data type representation from RelDataType.
     * @param type RelDataType.
     * @return DataType.
     */
    static DataType fromType(RelDataType type) {
        if (type.isStruct())
            return StructType.fromType(type);

        if (type instanceof RelDataTypeFactoryImpl.JavaType)
            return JavaType.fromType(type);

        return SqlType.fromType(type);
    }

    /**
     * Perform back conversion from data type representation to RelDataType itself.
     *
     * @param factory Type factory.
     * @return RelDataType.
     */
    RelDataType toRelDataType(RelDataTypeFactory factory);
}

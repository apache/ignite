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

package org.apache.ignite.internal.binary;

import org.apache.ignite.binary.BinaryIdMapper;
import org.apache.ignite.binary.BinaryNameMapper;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Internal binary mapper.
 */
public class BinaryInternalMapper {
    /** */
    private final BinaryNameMapper nameMapper;

    /** */
    private final BinaryIdMapper idMapper;

    /**
     * @param nameMapper Name mapper.
     * @param idMapper Id mapper.
     */
    public BinaryInternalMapper(BinaryNameMapper nameMapper, BinaryIdMapper idMapper) {
        assert nameMapper != null;
        assert idMapper != null;

        this.nameMapper = nameMapper;
        this.idMapper = idMapper;
    }

    /**
     * @return Name mapper.
     */
    public BinaryNameMapper nameMapper() {
        return nameMapper;
    }

    /**
     * @return ID mapper.
     */
    public BinaryIdMapper idMapper() {
        return idMapper;
    }

    /**
     * @param clsName Class name.
     * @return Type ID.
     */
    public int typeId(String clsName) {
        return idMapper.typeId(nameMapper.typeName(clsName));
    }

    /**
     * @param typeId Type ID.
     * @param fieldName Field name.
     * @return Field ID.
     */
    public int fieldId(int typeId, String fieldName) {
        return idMapper.fieldId(typeId, nameMapper.fieldName(fieldName));
    }

    /**
     * @param clsName Class name.
     * @return Type name.
     */
    public String typeName(String clsName) {
        return nameMapper.typeName(clsName);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(BinaryInternalMapper.class, this);
    }
}

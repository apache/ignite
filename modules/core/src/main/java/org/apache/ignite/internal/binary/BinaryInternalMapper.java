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

    /** */
    private boolean checkOnZeroId;

    /**
     * @param nameMapper Name mapper.
     * @param idMapper Id mapper.
     * @param checkOnZeroId Whether checks on zero id or not.
     */
    public BinaryInternalMapper(BinaryNameMapper nameMapper, BinaryIdMapper idMapper, boolean checkOnZeroId) {
        assert nameMapper != null;
        assert idMapper != null;

        this.nameMapper = nameMapper;
        this.idMapper = idMapper;
        this.checkOnZeroId = checkOnZeroId;
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
        int id = idMapper.typeId(nameMapper.typeName(clsName));

        if (!checkOnZeroId)
            return id;

        return id != 0 ? id : BinaryContext.SIMPLE_NAME_LOWER_CASE_MAPPER.typeId(clsName);
    }

    /**
     * @param typeId Type ID.
     * @param fieldName Field name.
     * @return Field ID.
     */
    public int fieldId(int typeId, String fieldName) {
        int id = idMapper.fieldId(typeId, nameMapper.fieldName(fieldName));

        if (!checkOnZeroId)
            return id;

        return id != 0 ? id : BinaryContext.SIMPLE_NAME_LOWER_CASE_MAPPER.fieldId(typeId, fieldName);
    }

    /**
     * @param clsName Class name.
     * @return Type name.
     */
    public String typeName(String clsName) {
        return nameMapper.typeName(clsName);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof BinaryInternalMapper))
            return false;

        BinaryInternalMapper mapper = (BinaryInternalMapper)o;

        return checkOnZeroId == mapper.checkOnZeroId 
            && idMapper.equals(mapper.idMapper) 
            && nameMapper.equals(mapper.nameMapper);

    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = nameMapper.hashCode();
        
        res = 31 * res + idMapper.hashCode();
        
        res = 31 * res + (checkOnZeroId ? 1 : 0);
        
        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(BinaryInternalMapper.class, this);
    }
}

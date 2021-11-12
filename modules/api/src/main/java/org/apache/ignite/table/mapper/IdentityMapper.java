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

package org.apache.ignite.table.mapper;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Set;
import org.jetbrains.annotations.NotNull;

/**
 * Trivial mapper implementation that maps a column to a field with the same name.
 *
 * @param <T> Target type.
 */
class IdentityMapper<T> implements Mapper<T> {
    /** Target type. */
    private final Class<T> targetType;
    
    /** Class field names. */
    private final Set<String> fieldsNames;
    
    /**
     * Creates a mapper for given class.
     *
     * @param targetType Target class.
     */
    IdentityMapper(Class<T> targetType) {
        this.targetType = targetType;
        
        Field[] fields = targetType.getDeclaredFields();
        fieldsNames = new HashSet<>(fields.length);
        
        for (int i = 0; i < fields.length; i++) {
            //TODO IGNITE-15787 Filter out 'transient' fields.
            fieldsNames.add(fields[i].getName());
        }
    }
    
    /** {@inheritDoc} */
    @Override public Class<T> targetType() {
        return targetType;
    }
    
    /** {@inheritDoc} */
    @Override public String columnToField(@NotNull String columnName) {
        return fieldsNames.contains(columnName) ? columnName : null;
    }
}

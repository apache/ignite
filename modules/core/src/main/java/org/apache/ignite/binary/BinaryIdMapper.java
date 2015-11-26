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

package org.apache.ignite.binary;

/**
 * Type and field ID mapper for binary objects. Ignite never writes full
 * strings for field or type names. Instead, for performance reasons, Ignite
 * writes integer hash codes for type and field names. It has been tested that
 * hash code conflicts for the type names or the field names
 * within the same type are virtually non-existent and, to gain performance, it is safe
 * to work with hash codes. For the cases when hash codes for different types or fields
 * actually do collide {@code BinaryIdMapper} allows to override the automatically
 * generated hash code IDs for the type and field names.
 * <p>
 * Binary ID mapper can be configured for all binary objects via
 * {@link org.apache.ignite.configuration.BinaryConfiguration#getIdMapper()} method,
 * or for a specific binary type via {@link BinaryTypeConfiguration#getIdMapper()} method.
 */
public interface BinaryIdMapper {
    /**
     * Gets type ID for provided class name.
     * <p>
     * If {@code 0} is returned, hash code of class simple name will be used.
     *
     * @param clsName Class name.
     * @return Type ID.
     */
    public int typeId(String clsName);

    /**
     * Gets ID for provided field.
     * <p>
     * If {@code 0} is returned, hash code of field name will be used.
     *
     * @param typeId Type ID.
     * @param fieldName Field name.
     * @return Field ID.
     */
    public int fieldId(int typeId, String fieldName);
}
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

import java.util.Collection;

/**
 * Binary type meta data. Metadata for binary types can be accessed from any of the
 * {@link org.apache.ignite.IgniteBinary#type(String)} methods.
 * Having metadata also allows for proper formatting of {@code BinaryObject#toString()} method,
 * even when binary objects are kept in binary format only, which may be necessary for audit reasons.
 */
public interface BinaryType {
    /**
     * Gets binary type name.
     *
     * @return Binary type name.
     */
    public String typeName();

    /**
     * Gets binary type ID.
     *
     * @return Binary type ID.
     */
    public int typeId();

    /**
     * Gets collection of all field names for this binary type.
     *
     * @return Collection of all field names for this binary type.
     */
    public Collection<String> fieldNames();

    /**
     * Gets name of the field type for a given field.
     *
     * @param fieldName Field name.
     * @return Field type name.
     */
    public String fieldTypeName(String fieldName);

    /**
     * Get {@link BinaryField} for the given field name. Later this field can be used for fast field's value
     * retrieval from concrete {@link BinaryObject}.
     *
     * @param fieldName Field name.
     * @return Binary field.
     */
    public BinaryField field(String fieldName);

    /**
     * Binary objects can optionally specify custom key-affinity mapping in the
     * configuration. This method returns the name of the field which should be
     * used for the key-affinity mapping.
     *
     * @return Affinity key field name.
     */
    public String affinityKeyFieldName();

    /**
     * Check whether type represents enum or not.
     *
     * @return {@code True} if type is enum.
     */
    public boolean isEnum();

    /**
     * @return Collection of enum values.
     */
    public Collection<BinaryObject> enumValues();
}

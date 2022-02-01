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

package org.apache.ignite.internal.processors.cache;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Object that contains serialized values for fields marked with {@link org.apache.ignite.configuration.SerializeSeparately}
 * in {@link org.apache.ignite.configuration.CacheConfiguration}.
 * This object is needed to exchange and store shrinked cache configurations to avoid possible {@link ClassNotFoundException} errors
 * during deserialization on nodes where some specific class may not exist.
 */
public class CacheConfigurationEnrichment implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Field name -> Field serialized value. */
    private final Map<String, byte[]> enrichFields;

    /** Field name -> Field value class name. */
    @GridToStringInclude
    private final Map<String, String> fieldClassNames;

    /**
     * Creates a new instance of CacheConfigurationEnrichment.
     *
     * @param enrichFields Mapping a field name to its serialized value.
     * @param fieldClassNames Field class names.
     */
    public CacheConfigurationEnrichment(
        Map<String, byte[]> enrichFields,
        Map<String, String> fieldClassNames
    ) {
        this.enrichFields = enrichFields;
        this.fieldClassNames = fieldClassNames;
    }

    /**
     * @param fieldName Field name.
     * @return Serialized value of the given field.
     */
    public byte[] getFieldSerializedValue(String fieldName) {
        return enrichFields.get(fieldName);
    }

    /**
     * Returns all field names that can be potentially enriched.
     *
     * @return Set of field names.
     */
    public Set<String> fields() {
        return fieldClassNames.keySet();
    }

    /**
     * @param fieldName Field name.
     * @return Class name of the given field.
     */
    public String getFieldClassName(String fieldName) {
        return fieldClassNames.get(fieldName);
    }

    /**
     * Returns {@code true} if this enrichment contains serialized valued for the specified field.
     *
     * @param name Field name.
     * @return True when field presents, false otherwise.
     */
    public boolean hasField(String name) {
        return fieldClassNames.containsKey(name);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheConfigurationEnrichment.class, this);
    }
}

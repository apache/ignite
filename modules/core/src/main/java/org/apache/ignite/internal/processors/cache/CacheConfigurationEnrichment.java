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

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Map;
import org.apache.ignite.IgniteException;

/**
 * Object that contains serialized values for fields marked with {@link org.apache.ignite.configuration.SerializeSeparately}
 * in {@link org.apache.ignite.configuration.CacheConfiguration}.
 * This object is needed to exchange and store shrinked cache configurations to avoid possible {@link ClassNotFoundException} errors
 * during deserialization on nodes where some specific class may not exist.
 */
public class CacheConfigurationEnrichment implements Serializable {
    /** Field name -> field serialized value. */
    private final Map<String, byte[]> enrichFields;

    /** Field name -> Field value class name. */
    private final Map<String, String> fieldClassNames;

    /**
     * @param enrichFields Enrich fields.
     * @param fieldClassNames Field class names.
     */
    public CacheConfigurationEnrichment(Map<String, byte[]> enrichFields, Map<String, String> fieldClassNames) {
        this.enrichFields = enrichFields;
        this.fieldClassNames = fieldClassNames;
    }

    /**
     * @param fieldName Field name.
     */
    public Object getFieldValue(String fieldName) {
        byte[] serializedVal = enrichFields.get(fieldName);

        ByteArrayInputStream bis = new ByteArrayInputStream(serializedVal);

        try (ObjectInputStream is = new ObjectInputStream(bis)) {
            return is.readObject();
        }
        catch (Exception e) {
            throw new IgniteException("Failed to deserialize field " + fieldName);
        }
    }

    /**
     * @param fieldName Field name.
     */
    public String getFieldClassName(String fieldName) {
        return fieldClassNames.get(fieldName);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "CacheConfigurationEnrichment{" +
            "enrichFields=" + enrichFields +
            '}';
    }
}

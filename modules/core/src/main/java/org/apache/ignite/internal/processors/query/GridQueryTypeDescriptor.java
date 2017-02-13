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

package org.apache.ignite.internal.processors.query;

import java.util.Map;
import org.apache.ignite.IgniteCheckedException;

/**
 * Value descriptor which allows to extract fields from value object of given type.
 */
public interface GridQueryTypeDescriptor {
    /**
     * Gets type name which uniquely identifies this type.
     *
     * @return Type name which uniquely identifies this type.
     */
    public String name();

    /**
     * Gets table name for type.
     *
     * @return Table name.
     */
    public String tableName();

    /**
     * Gets mapping from field name to its type.
     *
     * @return Fields that can be indexed, participate in queries and can be queried using method.
     */
    public Map<String, Class<?>> fields();

    /**
     * Gets field value for given key and value.
     *
     * @param field Field name.
     * @param key Key.
     * @param val Value.
     * @return Value for given field.
     * @throws IgniteCheckedException If failed.
     */
    public <T> T value(String field, Object key, Object val) throws IgniteCheckedException;

    /**
     * Sets field value for given key and value.
     *
     * @param field Field name.
     * @param key Key.
     * @param val Value.
     * @param propVal Value for given field.
     * @throws IgniteCheckedException If failed.
     */
    public void setValue(String field, Object key, Object val, Object propVal) throws IgniteCheckedException;

    /**
     * @param name Property name.
     * @return Property.
     */
    public GridQueryProperty property(String name);

    /**
     * Gets indexes for this type.
     *
     * @return Indexes for this type.
     */
    public Map<String, GridQueryIndexDescriptor> indexes();

    /**
     * Gets value class.
     *
     * @return Value class.
     */
    public Class<?> valueClass();

    /**
     * Gets key class.
     *
     * @return Key class.
     */
    public Class<?> keyClass();

    /**
     * Gets key type name.
     *
     * @return Key type name.
     */
    public String keyTypeName();

    /**
     * Gets value type name.
     *
     * @return Value type name.
     */
    public String valueTypeName();

    /**
     * Returns {@code true} if string representation of value should be indexed as text.
     *
     * @return If string representation of value should be full-text indexed.
     */
    public boolean valueTextIndex();

    /**
     * Returns affinity key field name or {@code null} for default.
     *
     * @return Affinity key.
     */
    public String affinityKey();
}
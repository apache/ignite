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

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.util.lang.IgnitePair;
import org.jetbrains.annotations.Nullable;

/**
 * Value descriptor which allows to extract fields from value object of given type.
 */
public interface GridQueryTypeDescriptor {
    /**
     * Gets cache name of this type.
     *
     * @return Cache name.
     */
    public String cacheName();

    /**
     * Gets type name which uniquely identifies this type.
     *
     * @return Type name which uniquely identifies this type.
     */
    public String name();

    /**
     * Gets schema name for type (database schema means here).
     *
     * @return Schema name.
     */
    public String schemaName();

    /**
     * Gets table name for type.
     *
     * @return Table name.
     */
    public String tableName();

    /**
     * Gets mapping from field name to its type. Field type can be a collection type with an element type.
     *
     * @return Fields that can be indexed, participate in queries and can be queried using method.
     */
    public Map<String, List<Class<?>>> fields();

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
     * @return All properties.
     */
    public Map<String, GridQueryProperty> properties();

    /**
     * Gets indexes for this type.
     *
     * @return Indexes for this type.
     */
    public Map<String, GridQueryIndexDescriptor> indexes();

    /**
     * Get text index for this type (if any).
     *
     * @return Text index or {@code null}.
     */
    public GridQueryIndexDescriptor textIndex();

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

    /**
     * @return Whether custom affinity key mapper exists.
     */
    public boolean customAffinityKeyMapper();

    /**
     * @return BinaryObject's type ID if indexed value is BinaryObject, otherwise value class' hash code.
     */
    public int typeId();

    /**
     * @param val Value cache object.
     * @return {@code true} If the type of the given value cache object matches this descriptor.
     */
    public boolean matchType(CacheObject val);

    /**
     * Gets key field name.
     * @return Key field name.
     */
    public String keyFieldName();

    /**
     * Gets value field name.
     * @return value field name.
     */
    public String valueFieldName();

    /**
     * Gets key field alias.
     *
     * @return Key field alias.
     */
    @Nullable public String keyFieldAlias();

    /**
     * Gets value field alias.
     *
     * @return value field alias.
     */
    @Nullable public String valueFieldAlias();

    /**
     * Performs validation of given key and value against configured constraints.
     * Throws runtime exception if validation fails.
     *
     * @param key Key.
     * @param val Value.
     * @throws IgniteCheckedException, If failure happens.
     */
    public void validateKeyAndValue(Object key, Object val) throws IgniteCheckedException;

    /**
     * Sets defaults value for given key and value.
     *
     * @param key Key.
     * @param val Value.
     * @throws IgniteCheckedException If failed.
     */
    public void setDefaults(Object key, Object val) throws IgniteCheckedException;

    /**
     * Gets primary key fields if defined, or empty collection otherwise.
     */
    public Set<String> primaryKeyFields();

    /**
     * Sets primary key fields.
     *
     * @param keys Primary keys.
     */
    public void primaryKeyFields(Set<String> keys);

    /**
     * Gets whether a primary key should be autogenerated.
     */
    public boolean implicitPk();

    /**
     * Sets whether a primary key should be autogenerated.
     *
     * @param implicitPk Whether a PK should be autogenerated.
     */
    public void implicitPk(boolean implicitPk);

    /**
     * @return {@code true} if absent PK parts should be filled with defaults, {@code false} otherwise.
     */
    public boolean fillAbsentPKsWithDefaults();

    /**
     * Sets up fillAbsentPKsWithDefaults flag.
     *
     * @param fillAbsentPKsWithDefaults Flag.
     */
    public void setFillAbsentPKsWithDefaults(boolean fillAbsentPKsWithDefaults);

    /**
     * Gets primary key index INLINE_SIZE.
     */
    public int primaryKeyInlineSize();

    /**
     * Sets primary key index INLINE_SIZE.
     */
    public void primaryKeyInlineSize(int pkInlineSize);

    /**
     * Gets affinity fields index INLINE_SIZE.
     */
    public int affinityFieldInlineSize();

    /**
     * Sets affinity fields index INLINE_SIZE.
     */
    public void affinityFieldInlineSize(int affFieldInlineSize);
}

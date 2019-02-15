/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.query;

import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.util.lang.GridMapEntry;
import org.jetbrains.annotations.Nullable;

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
     * @return BinaryObject's type ID if indexed value is BinaryObject, otherwise value class' hash code.
     */
    public int typeId();

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
}

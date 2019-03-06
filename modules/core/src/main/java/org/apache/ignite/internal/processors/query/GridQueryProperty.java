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

import org.apache.ignite.IgniteCheckedException;

/**
 * Description and access method for query entity field.
 */
public interface GridQueryProperty {
    /**
     * Gets this property value from the given object.
     *
     * @param key Key.
     * @param val Value.
     * @return Property value.
     * @throws IgniteCheckedException If failed.
     */
    public Object value(Object key, Object val) throws IgniteCheckedException;

    /**
     * Sets this property value for the given object.
     *
     * @param key Key.
     * @param val Value.
     * @param propVal Property value.
     * @throws IgniteCheckedException If failed.
     */
    public void setValue(Object key, Object val, Object propVal) throws IgniteCheckedException;

    /**
     * @return Property name.
     */
    public String name();

    /**
     * @return Class member type.
     */
    public Class<?> type();

    /**
     * Property ownership flag.
     * @return {@code true} if this property belongs to key, {@code false} if it belongs to value.
     */
    public boolean key();

    /**
     * @return Parent property or {@code null} if this property is not nested.
     */
    public GridQueryProperty parent();

    /**
     * Gets the flag restricting {@code null} value for this property.
     *
     * @return {@code true} if property does not allow {@code null} value.
     */
    public boolean notNull();

    /**
     * Gets the default value for this property.
     *
     * @return {@code null} if a default value is not set for the property.
     */
    public Object defaultValue();

    /**
     * Gets precision for this property.
     *
     * @return Precision for a decimal property or -1.
     */
    public int precision();

    /**
     * Gets scale for this property.
     *
     * @return Scale for a decimal property or -1.
     */
    public int scale();
}

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

/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.property;

import org.apache.ignite.IgniteCheckedException;

/**
 * Way of accessing a property - either via field or getter and setter methods.
 */
public interface QueryPropertyAccessor {
    /**
     * Get property value from given object.
     *
     * @param obj Object to retrieve property value from.
     * @return Property value.
     * @throws IgniteCheckedException if failed.
     */
    public Object getValue(Object obj) throws IgniteCheckedException;

    /**
     * Set property value on given object.
     *
     * @param obj Object to set property value on.
     * @param newVal Property value.
     * @throws IgniteCheckedException if failed.
     */
    public void setValue(Object obj, Object newVal)throws IgniteCheckedException;

    /**
     * @return Name of this property.
     */
    public String getPropertyName();

    /**
     * @return Type of the value of this property.
     */
    public Class<?> getType();
}

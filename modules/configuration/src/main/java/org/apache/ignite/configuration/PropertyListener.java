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

package org.apache.ignite.configuration;

import java.io.Serializable;

/**
 * Configuration property change listener.
 *
 * @param <VIEW> VIEW type of property.
 * @param <CHANGE> CHANGE type of property.
 */
public interface PropertyListener<VIEW extends Serializable, CHANGE extends Serializable> {
    /**
     * Called before property value is updated.
     *
     * @param oldValue Previous value.
     * @param newValue New value.
     * @param modifier Property itself.
     * @return {@code true} if changes are approved and {@code false} then property update must be aborted.
     */
    default boolean beforeUpdate(VIEW oldValue, VIEW newValue, ConfigurationProperty<VIEW, CHANGE> modifier) {
        return true;
    }

    /**
     * Called on property value update.
     *
     * @param newValue New value of the property.
     * @param modifier Property itself.
     */
    default void update(VIEW newValue, ConfigurationProperty<VIEW, CHANGE> modifier) {
        /* No-op */
    }

    /**
     * Called after property value update.
     *
     * @param newValue New value of the property.
     * @param modifier Property itself.
     */
    default void afterUpdate(VIEW newValue, ConfigurationProperty<VIEW, CHANGE> modifier) {
        /* No-op */
    }

}

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

import org.apache.ignite.configuration.validation.ConfigurationValidationException;

/**
 * Base interface for configuration.
 * @param <VALUE> Type of the value.
 * @param <CHANGE> Type of the object that changes the value of configuration.
 */
public interface ConfigurationProperty<VALUE, CHANGE> {
    /**
     * Get key of this node.
     * @return Key.
     */
    String key();

    /**
     * Get value of this property.
     * @return Value of this property.
     */
    VALUE value();

    /**
     * Change this configuration node value.
     * @param change CHANGE object.
     * @throws ConfigurationValidationException If validation failed.
     */
    void change(CHANGE change) throws ConfigurationValidationException;
}

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

package org.apache.ignite.spi.systemview.view;

import org.apache.ignite.internal.managers.systemview.walker.Order;

/**
 * Configuration value representation for a {@link SystemView}.
 */
public class ConfigurationView {
    /** Name of the configuration property. */
    private final String name;

    /** Value of the configuration property. */
    private final String val;

    /**
     * @param name Name of the configuration property.
     * @param val Value of the configuration property.
     */
    public ConfigurationView(String name, String val) {
        this.name = name;
        this.val = val;
    }

    /** @return Name of the configuration property. */
    @Order
    public String name() {
        return name;
    }

    /** @return Value of the configuration property. */
    public String value() {
        return val;
    }
}

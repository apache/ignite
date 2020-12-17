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

package org.apache.ignite.configuration.processor.internal;

import com.squareup.javapoet.TypeName;

/**
 * Configuration element with a reference to its parent and with an original name.
 */
public class ConfigurationNode extends ConfigurationElement {
    /** Configuration parent. */
    private final ConfigurationNode parent;

    /** Original name of configuration element. */
    private final String originalName;

    /** Constructor. */
    public ConfigurationNode(TypeName type, String name, String originalName, TypeName view, TypeName init, TypeName change, ConfigurationNode parent) {
        super(type, name, view, init, change);
        this.originalName = originalName;
        this.parent = parent;
    }

    /**
     * Get configuration parent.
     */
    public ConfigurationNode getParent() {
        return parent;
    }

    /**
     * Get original name of configuration element.
     */
    public String getOriginalName() {
        return originalName;
    }

}

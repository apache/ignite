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

package org.apache.ignite.configuration.internal.asm;

import org.apache.ignite.configuration.internal.DynamicConfiguration;
import org.apache.ignite.configuration.tree.InnerNode;

/**
 * Class to cache compiled classes and hold precalculated names to reference other existing classes.
 */
class SchemaClassesInfo {
    /** Configuration Schema class. */
    public final Class<?> schemaClass;

    /** Class name for the VIEW class. */
    public final String viewClassName;

    /** Class name for the CHANGE class. */
    public final String changeClassName;

    /** Class name for the Configuration class. */
    public final String cfgClassName;

    /** Class name for the Node class. */
    public final String nodeClassName;

    /** Class name for the Configuration Impl class. */
    public final String cfgImplClassName;

    /** Node class instance. */
    public Class<? extends InnerNode> nodeClass;

    /** Configuration Impl class instance. */
    public Class<? extends DynamicConfiguration<?, ?>> cfgImplClass;

    /**
     * @param schemaClass Configuration Schema class instance.
     */
    SchemaClassesInfo(Class<?> schemaClass) {
        this.schemaClass = schemaClass;
        String schemaClassName = schemaClass.getPackageName() + "." + schemaClass.getSimpleName(); // Support inner classes.

        String prefix = schemaClassName.replaceAll("ConfigurationSchema$", "");

        viewClassName = prefix + "View";
        changeClassName = prefix + "Change";
        cfgClassName = prefix + "Configuration";

        nodeClassName = prefix + "Node";
        cfgImplClassName = prefix + "ConfigurationImpl";
    }
}

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

package org.apache.ignite.internal.configuration.asm;

import org.apache.ignite.configuration.annotation.DirectAccess;
import org.apache.ignite.internal.configuration.DynamicConfiguration;
import org.apache.ignite.internal.configuration.tree.InnerNode;

/**
 * Class to cache compiled classes and hold precalculated names to reference other existing classes.
 */
class SchemaClassesInfo {
    /** Configuration class name postfix. */
    private static final String CONFIGURATION_CLASS_POSTFIX = "Configuration";

    /** View class name postfix. */
    private static final String VIEW_CLASS_POSTFIX = "View";

    /** Change class name postfix. */
    private static final String CHANGE_CLASS_POSTFIX = "Change";

    /** Configuration Schema class. */
    final Class<?> schemaClass;

    /** Flag indicating that this schema is annotated with {@link DirectAccess}. */
    final boolean direct;

    /** Class name for the VIEW class. */
    final String viewClassName;

    /** Class name for the CHANGE class. */
    final String changeClassName;

    /** Class name for the Configuration class. */
    final String cfgClassName;

    /** Class name for the Node class. */
    final String nodeClassName;

    /** Class name for the Configuration Impl class. */
    final String cfgImplClassName;

    /** Node class instance. */
    Class<? extends InnerNode> nodeClass;

    /** Configuration Impl class instance. */
    Class<? extends DynamicConfiguration<?, ?>> cfgImplClass;

    /**
     * Constructor.
     *
     * @param schemaClass Configuration Schema class instance.
     */
    SchemaClassesInfo(Class<?> schemaClass) {
        this.schemaClass = schemaClass;
        this.direct = schemaClass.isAnnotationPresent(DirectAccess.class);

        String prefix = prefix(schemaClass);

        viewClassName = prefix + VIEW_CLASS_POSTFIX;
        changeClassName = prefix + CHANGE_CLASS_POSTFIX;
        cfgClassName = prefix + CONFIGURATION_CLASS_POSTFIX;

        nodeClassName = prefix + "Node";
        cfgImplClassName = prefix + "ConfigurationImpl";
    }

    /**
     * Get the prefix for inner classes.
     * <p>
     * Example: org.apache.ignite.NodeConfigurationSchema -> org.apache.ignite.Node
     *
     * @param schemaClass Configuration schema class.
     * @return Prefix for inner classes.
     */
    private static String prefix(Class<?> schemaClass) {
        String schemaClassName = schemaClass.getPackageName() + "." + schemaClass.getSimpleName();

        return schemaClassName.replaceAll("ConfigurationSchema$", "");
    }

    /**
     * Get class name for the VIEW class.
     *
     * @param schemaClass Configuration schema class.
     * @return Class name for the VIEW class.
     */
    static String viewClassName(Class<?> schemaClass) {
        return prefix(schemaClass) + VIEW_CLASS_POSTFIX;
    }

    /**
     * Get class name for the CHANGE class.
     *
     * @param schemaClass Configuration schema class.
     * @return Class name for the CHANGE class.
     */
    static String changeClassName(Class<?> schemaClass) {
        return prefix(schemaClass) + CHANGE_CLASS_POSTFIX;
    }

    /**
     * Get class name for the Configuration class.
     *
     * @param schemaClass Configuration schema class.
     * @return Class name for the Configuration class.
     */
    static String configurationClassName(Class<?> schemaClass) {
        return prefix(schemaClass) + CONFIGURATION_CLASS_POSTFIX;
    }
}

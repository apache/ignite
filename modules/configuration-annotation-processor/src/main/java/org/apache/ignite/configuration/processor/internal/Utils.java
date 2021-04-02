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

import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import org.apache.ignite.configuration.internal.NamedListConfiguration;

/**
 * Annotation processing utilities.
 */
public class Utils {
    /** */
    private static final ClassName NAMED_LIST_CFG_CLASSNAME = ClassName.get("org.apache.ignite.configuration.internal", "NamedListConfiguration");

    /** Private constructor. */
    private Utils() {
    }

    /**
     * Get {@link ClassName} for configuration class.
     *
     * @param schemaClassName Configuration schema ClassName.
     * @return Configuration ClassName.
     */
    public static ClassName getConfigurationName(ClassName schemaClassName) {
        return ClassName.get(
            schemaClassName.packageName(),
            schemaClassName.simpleName().replace("Schema", "Impl")
        );
    }

    /**
     * Get {@link ClassName} for configuration class' public interface.
     *
     * @param schemaClassName Configuration schema ClassName.
     * @return Configuration's public interface ClassName.
     */
    public static ClassName getConfigurationInterfaceName(ClassName schemaClassName) {
        return ClassName.get(
            schemaClassName.packageName(),
            schemaClassName.simpleName().replace("Schema", "")
        );
    }

    /** */
    public static ClassName getNodeName(ClassName schemaClassName) {
        return ClassName.get(
            schemaClassName.packageName(),
            schemaClassName.simpleName().replace("ConfigurationSchema", "Node")
        );
    }

    /**
     * Get {@link ClassName} for configuration VIEW object class.
     *
     * @param schemaClassName Configuration schema ClassName.
     * @return Configuration VIEW object ClassName.
     */
    public static ClassName getViewName(ClassName schemaClassName) {
        return ClassName.get(
            schemaClassName.packageName(),
            schemaClassName.simpleName().replace("ConfigurationSchema", "View")
        );
    }

    /**
     * Get {@link ClassName} for configuration INIT object class.
     *
     * @param schemaClassName Configuration schema ClassName.
     * @return Configuration INIT object ClassName.
     */
    public static ClassName getInitName(ClassName schemaClassName) {
        return ClassName.get(
            schemaClassName.packageName(),
            schemaClassName.simpleName().replace("ConfigurationSchema", "Init")
        );
    }

    /**
     * Get {@link ClassName} for configuration CHANGE object class.
     *
     * @param schemaClassName Configuration schema ClassName.
     * @return Configuration CHANGE object ClassName.
     */
    public static ClassName getChangeName(ClassName schemaClassName) {
        return ClassName.get(
            schemaClassName.packageName(),
            schemaClassName.simpleName().replace("ConfigurationSchema", "Change")
        );
    }

    /**
     * Check whether type is {@link NamedListConfiguration}.
     *
     * @param type Type.
     * @return {@code true} if type is {@link NamedListConfiguration}.
     */
    public static boolean isNamedConfiguration(TypeName type) {
        if (type instanceof ParameterizedTypeName) {
            ParameterizedTypeName parameterizedTypeName = (ParameterizedTypeName) type;

            if (parameterizedTypeName.rawType.equals(NAMED_LIST_CFG_CLASSNAME))
                return true;
        }
        return false;
    }

    /**
     * @return {@code @SuppressWarnings("unchecked")} annotation spec object.
     */
    public static AnnotationSpec suppressWarningsUnchecked() {
        return AnnotationSpec.builder(SuppressWarnings.class)
            .addMember("value", "$S", "unchecked")
            .build();
    }
}

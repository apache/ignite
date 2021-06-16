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
package org.apache.ignite.internal.configuration.processor;

import com.squareup.javapoet.ClassName;

/**
 * Annotation processing utilities.
 */
public class Utils {
    /** Private constructor. */
    private Utils() {
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
            schemaClassName.simpleName().replaceAll("Schema$", "")
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
}

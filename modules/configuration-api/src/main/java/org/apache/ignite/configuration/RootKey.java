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

import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.storage.ConfigurationType;

/** */
public class RootKey<T extends ConfigurationTree<VIEW, ?>, VIEW> {
    /** */
    private final String rootName;

    /** */
    private final ConfigurationType storageType;

    /** */
    private final Class<?> schemaClass;

    /**
     * @param schemaClass Class of the configuration schema.
     */
    public RootKey(Class<?> schemaClass) {
        this.schemaClass = schemaClass;

        ConfigurationRoot rootAnnotation = schemaClass.getAnnotation(ConfigurationRoot.class);

        assert rootAnnotation != null;

        this.rootName = rootAnnotation.rootName();
        this.storageType = rootAnnotation.type();
    }

    /**
     * @return Name of the configuration root.
     */
    public String key() {
        return rootName;
    }

    /**
     * @return Configuration type of the root.
     */
    public ConfigurationType type() {
        return storageType;
    }

    /**
     * @return Schema class for the root.
     */
    public Class<?> schemaClass() {
        return schemaClass;
    }
}

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

package org.apache.ignite.internal.configuration;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;

import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.configuration.annotation.InternalConfiguration;
import org.apache.ignite.configuration.annotation.PolymorphicConfig;
import org.apache.ignite.configuration.validation.Validator;

/**
 * A module of configuration provided by a JAR file (or, in its source form, by a Maven/Gradle/... module).
 *
 * <p>Each configuration module only supplies configuration of only one {@link ConfigurationType}, so,
 * if a library needs to provide both node-local and cluster-wide configuration, it needs to supply
 * two ConfigurationModule instances.
 *
 * <p>Designed for integration with {@link java.util.ServiceLoader} mechanism, so ConfigurationModule instances
 * provided by a library are to be defined as services either in
 * {@code META-INF/services/org.apache.ignite.internal.configuration.ConfigurationModule}, or in a {@code module-info.java}.
 *
 * <p>Supplies the following configuration components:
 * <ul>
 *     <li><b>rootKeys</b> ({@link RootKey} instances)</li>
 *     <li><b>validators</b> ({@link Validator} instances)</li>
 *     <li><b>internalSchemaExtensions</b> (classes annotated with {@link InternalConfiguration})</li>
 *     <li><b>polymorphicSchemaExtensions</b> (classes annotataed with {@link PolymorphicConfig})</li>
 * </ul>
 *
 * @see ConfigurationType
 * @see RootKey
 * @see Validator
 * @see InternalConfiguration
 * @see PolymorphicConfig
 */
public interface ConfigurationModule {
    /**
     * Type of the configuration provided by this module.
     *
     * @return configuration type
     */
    ConfigurationType type();

    /**
     * Returns keys of configuration roots provided by this module.
     *
     * @return root keys
     */
    default Collection<RootKey<?, ?>> rootKeys() {
        return emptySet();
    }

    /**
     * Returns configuration validators provided by this module.
     *
     * @return configuration validators
     */
    default Map<Class<? extends Annotation>, Set<Validator<? extends Annotation, ?>>> validators() {
        return emptyMap();
    }

    /**
     * Returns classes of internal schema extensions (annotated with {@link InternalConfiguration})
     * provided by this module.
     *
     * @return internal schema extensions' classes
     */
    default Collection<Class<?>> internalSchemaExtensions() {
        return emptySet();
    }

    /**
     * Returns classes of polymorphic schema extensions (annotated with {@link PolymorphicConfig})
     * provided by this module.
     *
     * @return polymorphic schema extensions' classes
     */
    default Collection<Class<?>> polymorphicSchemaExtensions() {
        return emptySet();
    }
}

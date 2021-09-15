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

package org.apache.ignite.internal.configuration.testframework;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.apache.ignite.configuration.annotation.InternalConfiguration;
import org.apache.ignite.internal.configuration.ConfigurationChanger;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;

/**
 * Annotation for injecting configuration instances into tests.
 * <p/>
 * This annotation should be used on either fields or method parameters of the {@code *Configuration} type.
 * <p/>
 * Injected instance is initialized with values passed in {@link #value()}, with schema defaults where explicit initial
 * values are not found.
 * <p/>
 * Although configuration instance is mutable, there's no {@link ConfigurationRegistry} and {@link ConfigurationChanger}
 * underneath. Main point of the extension is to provide mocks.
 *
 * @see ConfigurationExtension
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.PARAMETER})
public @interface InjectConfiguration {
    /**
     * Configuration values to initialize the instance. Has HOCON syntax. Must have a root value {@code mock}.
     * <p/>
     * Examples:
     * <ul>
     *     <li>{@code mock.timeout=1000}</li>
     *     <li>{@code mock{cfg1=50, cfg2=90}}</li>
     * </ul>
     * <p/>
     * Uses only default values by default.
     *
     * @return Initial configuration values in HOCON format.
     */
    String value() default "mock : {}";

    /**
     * Array of configuration schema extensions. Every class in the array must be annotated with
     * {@link InternalConfiguration} and extend some public configuration.
     *
     * @return Array of configuration schema extensions.
     */
    Class<?>[] extensions() default {};
}

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

package org.apache.ignite.configuration.annotation;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * Annotation that marks underlying class as a root configuration schema. Has basically the same properties as {@link Config} + adds extra
 * properties.
 *
 * @see Config
 */
@Target(TYPE)
@Retention(RUNTIME)
@Documented
public @interface ConfigurationRoot {
    /**
     * Returns the unique root name.
     *
     * @return Unique root name.
     */
    String rootName();

    /**
     * Returns the type of the configuration.
     *
     * @return Type of the configuration.
     */
    ConfigurationType type() default ConfigurationType.LOCAL;
}

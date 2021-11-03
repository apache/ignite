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

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * This annotation marks the {@link PolymorphicConfig polymorphic configuration schema} field as a special (read only) leaf that will store
 * the current {@link PolymorphicConfigInstance#value polymorphic configuration type}.
 *
 * <p>NOTE: Field must be the first in the schema, and the type must be {@link String}.
 */
@Target(FIELD)
@Retention(RUNTIME)
@Documented
public @interface PolymorphicId {
    /**
     * Indicates that the field contains a default value that should be equal to one of the {@link PolymorphicConfigInstance#value}.
     *
     * @return {@code hasDefault} flag value.
     */
    boolean hasDefault() default false;
}

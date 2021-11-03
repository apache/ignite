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
 * This annotation marks the class as an instance of polymorphic configuration schema. Has basically the same properties as {@link Config},
 * but must inherit from the {@link PolymorphicConfig}.
 *
 * <p>NOTE: Field name conflicts with the parent ({@link PolymorphicConfig}) are not allowed.
 */
@Target(TYPE)
@Retention(RUNTIME)
@Documented
public @interface PolymorphicConfigInstance {
    /**
     * Unique identifier for an extension within a single {@link PolymorphicConfig polymorphic configuration}.
     *
     * @return Unique identifier for the extension of the polymorphic configuration instance.
     */
    String value();
}

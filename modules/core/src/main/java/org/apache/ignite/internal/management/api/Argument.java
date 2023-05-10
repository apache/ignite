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

package org.apache.ignite.internal.management.api;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Fields marked with this annotation is command arguments.
 * @see Command
 * @see CommandsRegistry
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Argument {
    /** @return {@code True} if argument optional, {@code false} if required. */
    public boolean optional() default false;

    /** @return Argument description. */
    public String description() default "";

    /** @return Argument example. If empty string returned then example will be generated automatically. */
    public String example() default "";

    /**
     * Required to keep compatibility with existing {@code control.sh} output.
     *
     * @return {@code True} if parameter example printed in help message must be formatted in java style.
     */
    public boolean javaStyleExample() default false;

    /**
     * Required to keep compatibility with existing {@code control.sh} output.
     *
     * @return {@code True} if paramter name in java style.
     */
    public boolean javaStyleName() default false;

    /**
     * Required to keep compatibility with existing {@code control.sh}.
     *
     * @return {@code True} if parameter name expected without "--" prefix.
     */
    public boolean withoutPrefix() default false;
}

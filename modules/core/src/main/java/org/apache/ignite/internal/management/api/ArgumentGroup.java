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
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Defines commands arguments restriction.
 * Group of {@link #value()} fields must be presented in Arguments.
 * If values from {@link #value()} not conform restrictions then error will be thrown.
 *
 * @see org.apache.ignite.internal.management.SystemViewCommandArg
 * @see ArgumentGroupsHolder
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Repeatable(ArgumentGroupsHolder.class)
public @interface ArgumentGroup {
    /** @return Names of argument class fields to forms "group" restriction. */
    public String[] value();

    /** @return {@code True} if arguments is optional, {@code false} if required. */
    public boolean optional();

    /** @return {@code True} if only one of argument from group allowed. */
    public boolean onlyOneOf() default false;
}

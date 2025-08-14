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

package org.apache.ignite.internal;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * The annotation specifies the position of a field in the serialized and deserialized byte sequence of a {@code Message} class.
 * <p>
 * The {@code value} indicates the index of the field in the serialization order.
 * Fields annotated with {@code @Order} are processed in ascending order of their index.
 * <p> By default, it is assumed that getters and setters are named as the annotated fields,
 * e.g. field 'val' should have getters and satters with name 'val' (according Ignite's to code-style).
 * If you need to override this behavior, you can specify their name in the {@link #method} attribute.
 * <p> This annotation must be used on non-static fields, and access to those fields
 * should be performed strictly through corresponding getter and setter methods
 * following the naming convention: {@code fieldName()} for getter and {@code fieldName(Type)} for setter.
 */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.FIELD)
public @interface Order {
    /** @return Order of the field. */
    int value();

    /** @return Getter and setter name. */
    String method() default "";
}

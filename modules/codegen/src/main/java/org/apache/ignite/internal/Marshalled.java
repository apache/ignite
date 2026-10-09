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
 * Links a message field to its wire form, held in the companion field(s) this annotation names. The marshalling
 * flavour is derived from the companion's shape:
 * <ul>
 *     <li>{@code byte[]} — the whole object is a single marshaller blob;</li>
 *     <li>{@code Message[]} — per-element {@code Message} serialization, the collection is rebuilt on unmarshal;</li>
 *     <li>{@code Collection<byte[]>} — per-element marshaller blobs, each element keeping its own class loader;</li>
 *     <li>{@link #keys()}/{@link #values()} instead of {@link #value()} — a {@code Map} serialized as two parallel
 *         wire fields.</li>
 * </ul>
 *
 * @see Order
 */
@Retention(RetentionPolicy.CLASS)
@Target(ElementType.FIELD)
public @interface Marshalled {
    /** Name of the companion wire field. Mutually exclusive with {@link #keys()}/{@link #values()}. */
    String value() default "";

    /** Name of the map-keys companion wire field; requires {@link #values()}. */
    String keys() default "";

    /** Name of the map-values companion wire field; requires {@link #keys()}. */
    String values() default "";
}

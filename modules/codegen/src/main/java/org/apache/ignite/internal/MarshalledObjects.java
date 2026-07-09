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
 * Marks a {@code Collection} field of arbitrary (non-{@code Message}) objects whose marshalled form is a companion
 * {@code @Order Collection<byte[]>} field named by {@link #value()}: each element is marshalled to its own
 * {@code byte[]}, so elements that need different deployment class loaders survive. Use {@code @MarshalledCollection}
 * instead when the elements are {@code Message}s.
 *
 * <p>An unmarshalled {@code Map.Entry} element (a query result row) gets its {@code KeyCacheObject} key resolved with
 * the message's cache object context: such a key arrives bytes-only and forbids lazy resolution.
 */
@Retention(RetentionPolicy.CLASS)
@Target(ElementType.FIELD)
public @interface MarshalledObjects {
    /** Name of the {@code @Order Collection<byte[]>} field that holds the per-element marshalled bytes. */
    String value();
}

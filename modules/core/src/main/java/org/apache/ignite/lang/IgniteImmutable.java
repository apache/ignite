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

package org.apache.ignite.lang;

import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;

import java.lang.annotation.*;

/**
 * Immutable types should be annotated with this annotation to avoid unnecessary
 * copying (e.g. in case of stateless compute closures or cache values).
 * <p>
 * If cache configuration flag {@link CacheConfiguration#isCopyOnRead()} is set
 * then for each operation implying return value copy of the value stored in cache is created.
 * Also if this flag is set copies are created for values passed to {@link CacheInterceptor} and
 * to {@link CacheEntryProcessor}.
 * <p>
 * Copies are not created for types marked with {@link IgniteImmutable} annotation and for known
 * immutable types:
 * <ul>
 *     <li>Boxed primitives ({@link Integer}, {@link Long}, ...)</li>
 *     <li>{@link String}</li>
 *     <li>{@link java.util.UUID}</li>
 *     <li>{@link java.math.BigDecimal}</li>
 *     <li>{@link org.apache.ignite.lang.IgniteUuid}</li>
 * </ul>
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface IgniteImmutable {
    // No-op.
}

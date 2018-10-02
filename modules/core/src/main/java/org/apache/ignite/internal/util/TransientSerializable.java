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

package org.apache.ignite.internal.util;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.apache.ignite.lang.IgniteProductVersion;

/**
 * Marks class as it has non-transient fields that should not be serialized.
 * Annotated class must have method that returns list of non-transient
 * fields that should not be serialized.
 * <p>
 *     Works only for jobs. For other messages node version is not available.
 * </p>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface TransientSerializable {
    /**
     * Name of the private static method that returns list of non-transient fields
     * that should not be serialized (String[]), and accepts {@link IgniteProductVersion}, e.g.
     * <pre>
     *     private static String[] fields(IgniteProductVersion ver){
     *         return ver.compareTo("1.5.30") < 0 ? EXCLUDED_FIELDS : null;
     *     }
     * </pre>
     * <p>
     *     On serialization version argument <tt>ver</tt> is receiver version and sender version on deserialization.
     * </p>
     * <p>
     *     If it returns empty array or null all non-transient fields will be normally serialized.
     * </p>
     *
     * @return Name of the method.
     */
    String methodName();
}

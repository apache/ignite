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

package org.apache.ignite.internal.network.serialization.marshal;

/**
 * Strategy for creating an empty (not yet filled with values) instance of a class.
 * Only used to instantiate proper classes, never gets interfaces, primitive classes and so on,
 * so implementations should not bother checking for them in {@link #supports(Class)}.
 */
interface Instantiation {
    /**
     * Returns {@code true} iff supports the provided class for means of instantiation.
     *
     * @param objectClass   class to check for support
     * @return {@code true} iff supports the provided class for means of instantiation
     */
    boolean supports(Class<?> objectClass);

    /**
     * Creates a new instance of the provided class.
     *
     * @param objectClass   class to instantiate
     * @return new instance of the given class
     * @throws InstantiationException   if something goes wrong during instantiation
     */
    Object newInstance(Class<?> objectClass) throws InstantiationException;
}

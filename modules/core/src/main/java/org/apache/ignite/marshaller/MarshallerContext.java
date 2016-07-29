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

package org.apache.ignite.marshaller;

import org.apache.ignite.IgniteCheckedException;

/**
 * Marshaller context.
 */
public interface MarshallerContext {
    /**
     * Registers class with provided type ID.
     *
     * @param id Type ID.
     * @param cls Class.
     * @return Whether class was registered.
     * @throws IgniteCheckedException In case of error.
     */
    public boolean registerClass(int id, Class cls) throws IgniteCheckedException;

    /**
     * Gets class for provided type ID.
     *
     * @param id Type ID.
     * @param ldr Class loader.
     * @return Class.
     * @throws ClassNotFoundException If class was not found.
     * @throws IgniteCheckedException In case of any other error.
     */
    public Class getClass(int id, ClassLoader ldr) throws ClassNotFoundException, IgniteCheckedException;

    /**
     * Checks whether the given type is a system one - JDK class or Ignite class.
     *
     * @param typeName Type name.
     * @return {@code true} if the type is a system one, {@code false} otherwise.
     */
    public boolean isSystemType(String typeName);
}
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
     * Method to register typeId->class name mapping in marshaller context <b>cluster-wide</b>.
     *
     * This method <b>guarantees</b> that mapping is delivered to all nodes in cluster
     * and blocks caller thread until then.
     *
     * @param platformId Id of a platform (java, .NET, etc.) to register mapping for.
     * @param typeId Type ID.
     * @param clsName Class name.
     * @return {@code True} if mapping was registered successfully.
     * @throws IgniteCheckedException In case of error.
     */
    public boolean registerClassName(byte platformId, int typeId, String clsName) throws IgniteCheckedException;

    /**
     * Method to register typeId->class name mapping in marshaller context <b>on local node only</b>.
     *
     * <b>No guarantees</b> that the mapping is presented on other nodes are provided.
     *
     * This method is safe to use if there is another source of mappings like metadata persisted on disk
     * and this source is known to be solid and free of conflicts beforehand.
     *
     * @param platformId Id of a platform (java, .NET, etc.) to register mapping for.
     * @param typeId Type id.
     * @param clsName Class name.
     * @return {@code True} if class mapping was registered successfully.
     * @throws IgniteCheckedException In case of error.
     */
    public boolean registerClassNameLocally(byte platformId, int typeId, String clsName) throws IgniteCheckedException;

    /**
     * Gets class for provided type ID.
     *
     * @param typeId Type ID.
     * @param ldr Class loader.
     * @return Class.
     * @throws ClassNotFoundException If class was not found.
     * @throws IgniteCheckedException In case of any other error.
     */
    public Class getClass(int typeId, ClassLoader ldr) throws ClassNotFoundException, IgniteCheckedException;


    /**
     * Gets class name for provided (platformId, typeId) pair.
     *
     * @param platformId id of a platform the class was registered for.
     * @param typeId Type ID.
     * @return Class name
     * @throws ClassNotFoundException If class was not found.
     * @throws IgniteCheckedException In case of any other error.
     */
    public String getClassName(byte platformId, int typeId) throws ClassNotFoundException, IgniteCheckedException;

    /**
     * Checks whether the given type is a system one - JDK class or Ignite class.
     *
     * @param typeName Type name.
     * @return {@code true} if the type is a system one, {@code false} otherwise.
     */
    public boolean isSystemType(String typeName);
}
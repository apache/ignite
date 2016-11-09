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
package org.apache.ignite.internal.processors.marshaller;

/**
 * Interface for abstracting concept of MappedName.
 * It is needed as local cache managed by {@link org.apache.ignite.internal.MarshallerContextImpl} may contain instances of two types:
 * <ul>
 *     <li>Regular {@link MappedNameImpl} with name itself and information whether this name was accepted by other nodes in the grid on not.</li>
 *     <li>{@link MappingRequestFuture} object representing situation when clients requests missing mapping from other nodes of grid (see {@link GridMarshallerMappingProcessor} for more information.)</li>
 * </ul>
 */
public interface MappedName {

    /**
     * @return full class name of mapped class.
     */
    String className();


    /**
     * @return true if mapping was accepted by all nodes in grid.
     */
    boolean isAccepted();
}

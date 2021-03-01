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

package org.apache.ignite.configuration.tree;

import java.util.NoSuchElementException;

/** */
public interface ConstructableTreeNode {
    /**
     * Initializes {@code key} element of the object with the content from the source.
     * Please refer to implementation to find out exact details.
     *
     * @param key Field / named list element name to be constructed.
     * @param src Source that provides data for construction.
     * @throws NoSuchElementException If {@code key} cannot be constructed.
     */
    void construct(String key,/* boolean canMutate, */ ConfigurationSource src) throws NoSuchElementException;

    /**
     * Public equivalent of {@link Object#clone()} method. Creates a copy with effectively the same content.
     * Helps to preserve trees immutability after construction is completed.
     *
     * @return Copy of the object.
     */
    ConstructableTreeNode copy();
}

/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.commandline.indexreader;

/**
 * This class is used for index tree traversal to store each tree's items. It's useful to have different storage
 * logic for different trees. See implementations of this interface for more info.
 */
interface ItemStorage<T> extends Iterable<T> {
    /** */
    void add(T item);

    /** */
    boolean contains(T item);

    /** */
    long size();
}
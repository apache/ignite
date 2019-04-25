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

package org.apache.ignite.internal.binary.streams;

/**
 * Binary stream.
 */
public interface BinaryStream {
    /**
     * @return Position.
     */
    public int position();

    /**
     * @param pos Position.
     */
    public void position(int pos);

    /**
     * @return Underlying array.
     */
    public byte[] array();

    /**
     * @return Copy of data in the stream.
     */
    public byte[] arrayCopy();

    /**
     * @return Offheap pointer if stream is offheap based and "forceHeap" flag is not set; otherwise {@code 0}.
     */
    public long offheapPointer();

    /**
     * @return Offheap pointer if stream is offheap based; otherwise {@code 0}.
     */
    public long rawOffheapPointer();

    /**
     * @return {@code True} is stream is array based.
     */
    public boolean hasArray();

    /**
     * @return Total capacity.
     */
    public int capacity();
}

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

package org.apache.ignite.hadoop.io;

/**
 * Memory abstraction for raw comparison.
 */
public interface RawMemory {
    /**
     * Get byte value at the given index.
     *
     * @param idx Index.
     * @return Value.
     */
    byte get(int idx);

    /**
     * Get short value at the given index.
     *
     * @param idx Index.
     * @return Value.
     */
    short getShort(int idx);

    /**
     * Get char value at the given index.
     *
     * @param idx Index.
     * @return Value.
     */
    char getChar(int idx);

    /**
     * Get int value at the given index.
     *
     * @param idx Index.
     * @return Value.
     */
    int getInt(int idx);

    /**
     * Get long value at the given index.
     *
     * @param idx Index.
     * @return Value.
     */
    long getLong(int idx);

    /**
     * Get float value at the given index.
     *
     * @param idx Index.
     * @return Value.
     */
    float getFloat(int idx);

    /**
     * Get double value at the given index.
     *
     * @param idx Index.
     * @return Value.
     */
    double getDouble(int idx);

    /**
     * Get length.
     *
     * @return Length.
     */
    int length();
}

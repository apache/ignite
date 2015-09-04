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

package org.apache.ignite.internal.processors.platform.memory;

/**
 * Interop memory chunk.
 */
public interface PlatformMemory extends AutoCloseable {
    /**
     * Gets input stream.
     *
     * @return Input stream.
     */
    public PlatformInputStream input();

    /**
     * Gets output stream.
     *
     * @return Output stream.
     */
    public PlatformOutputStream output();

    /**
     * Gets pointer which can be passed between platforms.
     *
     * @return Pointer.
     */
    public long pointer();

    /**
     * Gets data pointer.
     *
     * @return Data pointer.
     */
    public long data();

    /**
     * Gets capacity.
     *
     * @return Capacity.
     */
    public int capacity();

    /**
     * Gets length.
     *
     * @return Length.
     */
    public int length();

    /**
     * Reallocate memory chunk.
     *
     * @param cap Minimum capacity.
     */
    public void reallocate(int cap);

    /**
     * Close memory releasing it.
     */
    @Override void close();
}
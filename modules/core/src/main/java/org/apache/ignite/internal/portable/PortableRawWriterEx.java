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

package org.apache.ignite.internal.portable;

import org.apache.ignite.internal.portable.streams.PortableOutputStream;
import org.apache.ignite.internal.portable.api.PortableException;
import org.apache.ignite.internal.portable.api.PortableRawWriter;
import org.jetbrains.annotations.Nullable;

/**
 * Extended writer interface.
 */
public interface PortableRawWriterEx extends PortableRawWriter, AutoCloseable {
    /**
     * @param obj Object to write.
     * @throws PortableException In case of error.
     */
    public void writeObjectDetached(@Nullable Object obj) throws PortableException;

    /**
     * @return Output stream.
     */
    public PortableOutputStream out();

    /**
     * Cleans resources.
     */
    @Override public void close();

    /**
     * Reserve a room for an integer.
     *
     * @return Position in the stream where value is to be written.
     */
    public int reserveInt();

    /**
     * Write int value at the specific position.
     *
     * @param pos Position.
     * @param val Value.
     * @throws PortableException If failed.
     */
    public void writeInt(int pos, int val) throws PortableException;
}
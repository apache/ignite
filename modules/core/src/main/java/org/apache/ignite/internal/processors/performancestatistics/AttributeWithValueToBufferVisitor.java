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

package org.apache.ignite.internal.processors.performancestatistics;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.ignite.spi.systemview.view.SystemViewRowAttributeWalker;
import org.jetbrains.annotations.Nullable;

/** Fullfill {@code data} Map for specific row. */
class AttributeWithValueToBufferVisitor implements SystemViewRowAttributeWalker.AttributeWithValueVisitor {
    /** */
    private final ByteArrayOutputStream byteArrOutputStream = new ByteArrayOutputStream();

    /** */
    private final DataOutputStream dataOutputStream;

    public AttributeWithValueToBufferVisitor(ByteArrayOutputStream byteArrOutputStream) {
        dataOutputStream = new DataOutputStream(byteArrOutputStream);
    }

    /** */
    public void reset() {
        byteArrOutputStream.reset();
    }

    /** {@inheritDoc} */
    @Override public <T> void accept(int idx, String name, Class<T> clazz, @Nullable T val) {
        try {
            dataOutputStream.writeBytes(String.valueOf(val));
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void acceptBoolean(int idx, String name, boolean val) {
        try {
            dataOutputStream.writeBoolean(val);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void acceptChar(int idx, String name, char val) {
        try {
            dataOutputStream.writeChar(val);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void acceptByte(int idx, String name, byte val) {
        try {
            dataOutputStream.writeByte(val);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void acceptShort(int idx, String name, short val) {
        try {
            dataOutputStream.writeShort(val);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void acceptInt(int idx, String name, int val) {
        try {
            dataOutputStream.writeInt(val);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void acceptLong(int idx, String name, long val) {
        try {
            dataOutputStream.writeLong(val);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void acceptFloat(int idx, String name, float val) {
        try {
            dataOutputStream.writeFloat(val);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void acceptDouble(int idx, String name, double val) {
        try {
            dataOutputStream.writeDouble(val);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

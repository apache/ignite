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

package org.apache.ignite.internal.processors.hadoop.shuffle.collections;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.hadoop.HadoopSerialization;
import org.apache.ignite.internal.processors.hadoop.shuffle.mem.MemoryManager;
import org.apache.ignite.internal.processors.hadoop.shuffle.streams.HadoopDataInStream;

/**
 * Reader for key and value.
 */
class ReaderBase implements AutoCloseable {
    /** */
    private Object tmp;

    /** */
    private final HadoopSerialization ser;

    /** */
    private final HadoopDataInStream in;

    /** */
    private final MemoryManager mem;

    /**
     * @param ser Serialization.
     * @param mem Memory manger.
     */
    protected ReaderBase(HadoopSerialization ser, MemoryManager mem) {
        assert ser != null;
        assert mem != null;

        this.ser = ser;
        this.mem = mem;

        in = new HadoopDataInStream(mem);
    }

    /**
     * @param valPtr Value page pointer.
     * @return Value.
     */
    public Object readValue(long valPtr) {
        assert valPtr > 0 : valPtr;

        try {
            return read(valPtr + 12, mem.valueSize(valPtr));
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Resets temporary object to the given one.
     *
     * @param tmp Temporary object for reuse.
     */
    public void resetReusedObject(Object tmp) {
        this.tmp = tmp;
    }

    /**
     * @param ptr Pointer.
     * @param size Object size.
     * @return Object.
     * @throws IgniteCheckedException If failed.
     */
    protected Object read(long ptr, long size) throws IgniteCheckedException {
        in.buffer().set(ptr, size);

        tmp = ser.read(in, tmp);

        return tmp;
    }

    /** {@inheritDoc} */
    @Override public void close() throws IgniteCheckedException {
        ser.close();
    }
}

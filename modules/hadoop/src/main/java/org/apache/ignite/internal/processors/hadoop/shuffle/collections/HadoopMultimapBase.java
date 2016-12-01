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

import org.apache.ignite.internal.processors.hadoop.HadoopJobInfo;
import org.apache.ignite.internal.processors.hadoop.shuffle.mem.MemoryManager;

/**
 * Base class for all multimaps.
 */
public abstract class HadoopMultimapBase implements HadoopMultimap {
    /** */
    protected final MemoryManager mem;

    /**
     * @param jobInfo Job info.
     * @param mem Memory manager.n
     */
    protected HadoopMultimapBase(HadoopJobInfo jobInfo, MemoryManager mem) {
        assert jobInfo != null;
        assert mem != null;

        this.mem = mem;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        mem.close();
    }

    /**
     * @param valPtr Value page pointer.
     * @param nextValPtr Next value page pointer.
     */
    protected void nextValue(long valPtr, long nextValPtr) {
        mem.writeLong(valPtr, nextValPtr);
    }

    /**
     * @param valPtr Value page pointer.
     * @return Next value page pointer.
     */
    protected long nextValue(long valPtr) {
        return mem.readLong(valPtr);
    }

    /**
     * @param valPtr Value page pointer.
     * @param size Size.
     */
    protected void valueSize(long valPtr, int size) {
        mem.writeInt(valPtr + 8, size);
    }

    /**
     * @param valPtr Value page pointer.
     * @return Value size.
     */
    protected int valueSize(long valPtr) {
        return mem.readInt(valPtr + 8);
    }
}
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

package org.apache.ignite.internal.processors.platform.memory;

import org.apache.ignite.internal.util.GridUnsafe;

/**
 * Interop memory chunk abstraction.
 */
public abstract class PlatformAbstractMemory implements PlatformMemory {
    /** Stream factory. */
    private static final StreamFactory STREAM_FACTORY = GridUnsafe.BIG_ENDIAN ?
        new BigEndianStreamFactory() : new LittleEndianStreamFactory();

    /** Cross-platform memory pointer. */
    protected long memPtr;

    /**
     * Constructor.
     *
     * @param memPtr Cross-platform memory pointer.
     */
    protected PlatformAbstractMemory(long memPtr) {
        this.memPtr = memPtr;
    }

    /** {@inheritDoc} */
    @Override public PlatformInputStream input() {
        return STREAM_FACTORY.createInput(this);
    }

    /** {@inheritDoc} */
    @Override public PlatformOutputStream output() {
        return STREAM_FACTORY.createOutput(this);
    }

    /** {@inheritDoc} */
    @Override public long pointer() {
        return memPtr;
    }

    /** {@inheritDoc} */
    @Override public long data() {
        return PlatformMemoryUtils.data(memPtr);
    }

    /** {@inheritDoc} */
    @Override public int capacity() {
        return PlatformMemoryUtils.capacity(memPtr);
    }

    /** {@inheritDoc} */
    @Override public int length() {
        return PlatformMemoryUtils.length(memPtr);
    }

    /**
     * Stream factory.
     */
    private static interface StreamFactory {
        /**
         * Create input stream.
         *
         * @param mem Memory.
         * @return Input stream.
         */
        PlatformInputStreamImpl createInput(PlatformMemory mem);

        /**
         * Create output stream.
         *
         * @param mem Memory.
         * @return Output stream.
         */
        PlatformOutputStreamImpl createOutput(PlatformMemory mem);
    }

    /**
     * Stream factory for LITTLE ENDIAN architecture.
     */
    private static class LittleEndianStreamFactory implements StreamFactory {
        /** {@inheritDoc} */
        @Override public PlatformInputStreamImpl createInput(PlatformMemory mem) {
            return new PlatformInputStreamImpl(mem);
        }

        /** {@inheritDoc} */
        @Override public PlatformOutputStreamImpl createOutput(PlatformMemory mem) {
            return new PlatformOutputStreamImpl(mem);
        }
    }

    /**
     * Stream factory for BIG ENDIAN architecture.
     */
    private static class BigEndianStreamFactory implements StreamFactory {
        /** {@inheritDoc} */
        @Override public PlatformInputStreamImpl createInput(PlatformMemory mem) {
            return new PlatformBigEndianInputStreamImpl(mem);
        }

        /** {@inheritDoc} */
        @Override public PlatformOutputStreamImpl createOutput(PlatformMemory mem) {
            return new PlatformBigEndianOutputStreamImpl(mem);
        }
    }

}
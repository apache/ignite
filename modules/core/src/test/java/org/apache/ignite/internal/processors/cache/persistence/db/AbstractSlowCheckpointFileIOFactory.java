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

package org.apache.ignite.internal.processors.cache.persistence.db;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.OpenOption;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIODecorator;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;

/**
 * File I/O that emulates poor checkpoint write speed.
 */
public abstract class AbstractSlowCheckpointFileIOFactory implements FileIOFactory {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Delegate factory. */
    private final FileIOFactory delegateFactory = new RandomAccessFileIOFactory();

    /** Slow checkpoint enabled. */
    private final AtomicBoolean slowCheckpointEnabled;

    /** Checkpoint park nanos. */
    private final long checkpointParkNanos;

    /**
     * @param slowCheckpointEnabled Slow checkpoint enabled.
     * @param checkpointParkNanos Checkpoint park nanos.
     */
    protected AbstractSlowCheckpointFileIOFactory(AtomicBoolean slowCheckpointEnabled, long checkpointParkNanos) {
        this.slowCheckpointEnabled = slowCheckpointEnabled;
        this.checkpointParkNanos = checkpointParkNanos;
    }

    /** {@inheritDoc} */
    @Override public FileIO create(File file, OpenOption... openOption) throws IOException {
        final FileIO delegate = delegateFactory.create(file, openOption);

        return new FileIODecorator(delegate) {
            @Override public int write(ByteBuffer srcBuf) throws IOException {
                parkIfNeeded();

                return delegate.write(srcBuf);
            }

            @Override public int write(ByteBuffer srcBuf, long position) throws IOException {
                parkIfNeeded();

                return delegate.write(srcBuf, position);
            }

            @Override public int write(byte[] buf, int off, int len) throws IOException {
                parkIfNeeded();

                return delegate.write(buf, off, len);
            }

            /** Parks current checkpoint thread if slow mode is enabled. */
            private void parkIfNeeded() {
                if (slowCheckpointEnabled.get() && shouldSlowDownCurrentThread())
                    LockSupport.parkNanos(checkpointParkNanos);
            }
        };
    }

    /**
     * Returns {@code true} if the current thread should be slowed down.
     *
     * @return {@code true} if the current thread should be slowed down
     */
    protected abstract boolean shouldSlowDownCurrentThread();
}

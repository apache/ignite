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
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIODecorator;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;

/**
 *
 */
class CheckpointFailingIoFactory implements FileIOFactory {
    /** */
    private volatile boolean fail;

    /**
     * Will fail immediately.
     */
    public CheckpointFailingIoFactory() {
        this(true);
    }

    /**
     * @param failImmediately Fail immediately flag.
     */
    public CheckpointFailingIoFactory(boolean failImmediately) {
        fail = failImmediately;
    }

    /**
     * After this call all subsequent write calls will fail.
     */
    public void startFailing() {
        fail = true;
    }

    /** {@inheritDoc} */
    @Override public FileIO create(File file, OpenOption... modes) throws IOException {
        FileIO delegate = new RandomAccessFileIOFactory().create(file, modes);

        if (file.getName().contains("part-"))
            return new FileIODecorator(delegate) {
                @Override public int write(ByteBuffer srcBuf) throws IOException {
                    if (fail)
                        throw new IOException("test");
                    else
                        return super.write(srcBuf);
                }

                @Override public int write(ByteBuffer srcBuf, long position) throws IOException {
                    if (fail)
                        throw new IOException("test");
                    else
                        return super.write(srcBuf, position);
                }

                @Override public int write(byte[] buf, int off, int len) throws IOException {
                    if (fail)
                        throw new IOException("test");
                    else
                        return super.write(buf, off, len);
                }
            };

        return delegate;
    }
}

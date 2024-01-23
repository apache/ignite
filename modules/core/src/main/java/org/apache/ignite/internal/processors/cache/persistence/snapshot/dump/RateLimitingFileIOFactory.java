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

package org.apache.ignite.internal.processors.cache.persistence.snapshot.dump;

import java.io.File;
import java.io.IOException;
import java.nio.file.OpenOption;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.util.BasicRateLimiter;

/**
 * File I/O factory which provides {@link RateLimitingFileIO} implementation of FileIO.
 */
public class RateLimitingFileIOFactory implements FileIOFactory {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final FileIOFactory factory;

    /** */
    private final BasicRateLimiter rateLimiter;

    /** */
    public RateLimitingFileIOFactory(FileIOFactory factory, BasicRateLimiter rateLimiter) {
        this.factory = factory;
        this.rateLimiter = rateLimiter;
    }

    /** {@inheritDoc} */
    @Override public RateLimitingFileIO create(File file, OpenOption... modes) throws IOException {
        return new RateLimitingFileIO(factory.create(file, modes), rateLimiter);
    }
}

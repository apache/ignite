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
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * File I/O factory which provides {@link BufferedFileIO} implementation of FileIO.
 */
public class BufferedFileIOFactory implements FileIOFactory {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    protected final FileIOFactory factory;

    /** */
    public BufferedFileIOFactory(FileIOFactory factory) {
        this.factory = factory;
    }

    /** {@inheritDoc} */
    @Override public BufferedFileIO create(File file, OpenOption... modes) throws IOException {
        return new BufferedFileIO(factory.create(file, modes));
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(BufferedFileIOFactory.class, this);
    }
}

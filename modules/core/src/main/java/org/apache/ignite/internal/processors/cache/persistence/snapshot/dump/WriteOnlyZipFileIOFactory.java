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
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;

import static org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree.ZIP_SUFFIX;

/**
 * File I/O factory which provides {@link WriteOnlyZipFileIO} implementation of FileIO.
 */
public class WriteOnlyZipFileIOFactory extends BufferedFileIOFactory {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public WriteOnlyZipFileIOFactory(FileIOFactory factory) {
        super(factory);
    }

    /** {@inheritDoc} */
    @Override public WriteOnlyZipFileIO create(File file, OpenOption... modes) throws IOException {
        A.ensure(file.getName().endsWith(ZIP_SUFFIX), "File name should end with " + ZIP_SUFFIX);

        String entryName = file.getName().substring(0, file.getName().length() - ZIP_SUFFIX.length());

        return new WriteOnlyZipFileIO(factory.create(file, modes), entryName);
    }


    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(WriteOnlyZipFileIOFactory.class, this);
    }
}

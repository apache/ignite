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

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;

import static java.util.zip.Deflater.BEST_COMPRESSION;

/**
 * {@link FileIO} that allows to write ZIP compressed file.
 * It doesn't support reading or random access.
 * It is not designed for writing concurrently from several threads.
 */
public class WriteOnlyZipFileIO extends BufferedFileIO {
    /** */
    private final ZipOutputStream zos;

    /** */
    public WriteOnlyZipFileIO(FileIO fileIO, String entryName) throws IOException {
        super(fileIO);

        zos = new ZipOutputStream(new OutputStream() {
            @Override public void write(byte[] b, int off, int len) throws IOException {
                WriteOnlyZipFileIO.super.writeBytes(b, off, len);
            }

            @Override public void write(int b) throws IOException {
                writeByte((byte)b);
            }
        });

        zos.setLevel(BEST_COMPRESSION);

        zos.putNextEntry(new ZipEntry(entryName));
    }

    /** {@inheritDoc} */
    @Override protected void writeBytes(byte[] srcBuf, int off, int len) throws IOException {
        zos.write(srcBuf, off, len);
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        zos.close();

        super.close();
    }
}

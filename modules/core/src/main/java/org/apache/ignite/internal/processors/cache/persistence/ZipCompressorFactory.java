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

package org.apache.ignite.internal.processors.cache.persistence;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.Deflater;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

/**
 * Data stream compression and decompression. Used ZIP.
 */
public class ZipCompressorFactory implements CompressorFactory{
    /** */
    private static final long serialVersionUID = 7808218676954074199L;

    /** {@inheritDoc} */
    @Override public String filenameExtension() {
        return "zip";
    }

    /** {@inheritDoc} */
    @Override public OutputStream compress(OutputStream out) throws IOException {
        final ZipOutputStream result = new ZipOutputStream(out);
        result.setLevel(Deflater.BEST_SPEED);
        result.putNextEntry(new ZipEntry(""));
        return result;
    }

    /** {@inheritDoc} */
    @Override public InputStream decompress(InputStream in) throws IOException {
        final ZipInputStream result = new ZipInputStream(in);
        result.getNextEntry();
        return result;
    }
}

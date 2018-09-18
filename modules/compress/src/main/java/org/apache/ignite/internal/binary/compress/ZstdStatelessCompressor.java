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

package org.apache.ignite.internal.binary.compress;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdDictDecompress;
import java.util.Arrays;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CompressionConfiguration;

/** */
public class ZstdStatelessCompressor extends CompressorAdapter {
    /** */
    private volatile int level;

    @Override public void configure(CompressionConfiguration compressionCfg) {
        level = compressionCfg.getCompressionLevel();
    }

    public byte[] tryCompress(byte[] input) {
        byte[] compressed = compress(input, level);

        return appraiseAndAddHeader(input, compressed, 1);
    }

    public static byte[] compress(byte[] src, int level) {
        long maxDstSize = Zstd.compressBound(src.length);

        if (maxDstSize > Integer.MAX_VALUE) {
            throw new RuntimeException("Max output size is greater than MAX_INT");
        }

        byte[] dst = new byte[(int) maxDstSize + 1];
        long size = Zstd.compressByteArray(dst, 1, dst.length - 1, src, 0, src.length, level);

        if (Zstd.isError(size)) {
            throw new RuntimeException(Zstd.getErrorName(size));
        }

        return Arrays.copyOfRange(dst, 0, (int) size + 1);
    }

    @Override protected ZstdDictDecompress dictionary(byte dict) {
        throw new IgniteException("Header indicates dictionary use but compressor is stateless: " + dict);
    }
}

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

package org.apache.ignite.igfs.mapreduce.records;

import java.io.Externalizable;
import java.nio.charset.Charset;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Record resolver based on delimiters represented as strings. Works in the same way as
 * {@link IgfsByteDelimiterRecordResolver}, but uses strings as delimiters instead of byte arrays.
 */
public class IgfsStringDelimiterRecordResolver extends IgfsByteDelimiterRecordResolver {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Converts string delimiters to byte delimiters.
     *
     * @param charset Charset.
     * @param delims String delimiters.
     * @return Byte delimiters.
     */
    @Nullable private static byte[][] toBytes(Charset charset, @Nullable String... delims) {
        byte[][] res = null;

        if (delims != null) {
            res = new byte[delims.length][];

            for (int i = 0; i < delims.length; i++)
                res[i] = delims[i].getBytes(charset);
        }

        return res;
    }

    /**
     * Empty constructor required for {@link Externalizable} support.
     */
    public IgfsStringDelimiterRecordResolver() {
        // No-op.
    }

    /**
     * Creates record resolver from given string and given charset.
     *
     * @param delims Delimiters.
     * @param charset Charset.
     */
    public IgfsStringDelimiterRecordResolver(Charset charset, String... delims) {
        super(toBytes(charset, delims));
    }

    /**
     * Creates record resolver based on given string with default charset.
     *
     * @param delims Delimiters.
     */
    public IgfsStringDelimiterRecordResolver(String... delims) {
        super(toBytes(Charset.defaultCharset(), delims));
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsStringDelimiterRecordResolver.class, this);
    }
}
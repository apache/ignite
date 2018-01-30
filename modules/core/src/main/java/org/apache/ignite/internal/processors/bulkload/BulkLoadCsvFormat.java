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

package org.apache.ignite.internal.processors.bulkload;

import org.jetbrains.annotations.Nullable;

import java.nio.charset.Charset;
import java.util.regex.Pattern;

/** A placeholder for bulk load CSV format parser options. */
public class BulkLoadCsvFormat extends BulkLoadFormat {

    /** Line separator pattern. */
    public static final Pattern LINE_SEP_RE = Pattern.compile("[\r\n]+");

    /** Field separator pattern. */
    public static final Pattern FIELD_SEP_RE = Pattern.compile(",");

    /** Quote characters */
    public static final String QUOTE_CHARS = "\"";

    /** Format name. */
    public static final String NAME = "CSV";

    /** File charset. */
    @Nullable private Charset inputCharset;

    /**
     * Creates CSV format with default parameters.
     */
    public BulkLoadCsvFormat() {
    }

    /**
     * Returns the name of the format, null if not set.
     *
     * @return The name of the format, null if not set.
     */
    @Override public String name() {
        return NAME;
    }

    /**
     * Returns the input file charset, null if not set.
     *
     * @return The input file charset, null if not set.
     */
    public @Nullable Charset inputCharset() {
        return inputCharset;
    }

    /**
     * Sets the input file charset.
     *
     * @param charset The input file charset.
     */
    public void inputCharset(@Nullable Charset charset) {
        this.inputCharset = charset;
    }
}

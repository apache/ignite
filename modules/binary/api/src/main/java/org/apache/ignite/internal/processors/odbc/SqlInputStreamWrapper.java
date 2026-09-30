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

package org.apache.ignite.internal.processors.odbc;

import java.io.InputStream;

/**
 * InputStream wrapper used to pass it as argument to PreparedStatement.
 */
public class SqlInputStreamWrapper {
    /** Input stream wrapped. */
    private final InputStream stream;

    /** Length of data in the input stream. -1 if unknown. */
    private final int len;

    /**
     * @param stream Input stream.
     * @param len Length of data in the input stream. -1 if unknown.
     */
    public SqlInputStreamWrapper(InputStream stream, int len) {
        this.stream = stream;
        this.len = len;
    }

    /**
     * @param stream Input stream.
     */
    public SqlInputStreamWrapper(InputStream stream) {
        this(stream, -1);
    }

    /**
     * Returns input stream for the enclosed data.
     *
     * @return Input stream.
     */
    public InputStream inputStream() {
        return stream;
    }

    /**
     * @return Length of data in the input stream.
     */
    public int length() {
        return len;
    }
}

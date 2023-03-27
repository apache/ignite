/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.commandline;

import java.io.OutputStream;
import java.nio.charset.Charset;
import org.apache.ignite.internal.util.GridStringBuilder;

/**
 * Output stream that can be used to print some output to string builder.
 */
public class StringBuilderOutputStream extends OutputStream {
    /** */
    private final GridStringBuilder sb;

    /** */
    protected final Charset encoding;

    /** */
    public StringBuilderOutputStream() {
        this(new GridStringBuilder(), Charset.forName("UTF-8"));
    }

    /** */
    public StringBuilderOutputStream(GridStringBuilder sb) {
        this(sb, Charset.forName("UTF-8"));
    }

    /** */
    public StringBuilderOutputStream(GridStringBuilder sb, Charset encoding) {
        this.sb = sb;
        this.encoding = encoding;
    }

    /** {@inheritDoc} */
    @Override public void write(int b) {
        sb.a((char)b);
    }

    /** {@inheritDoc} */
    @Override public void write(byte b[]) {
        sb.a(new String(b, encoding));
    }

    /** {@inheritDoc} */
    @Override public void write(byte b[], int off, int len) {
        sb.a(new String(b, off, len, encoding));
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return this.sb.toString();
    }
}

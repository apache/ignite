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
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Record resolver based on new line detection. This resolver can detect new lines based on '\n' or '\r\n' sequences.
 * <p>
 * Note that this resolver cannot be created and has one constant implementations: {@link #NEW_LINE}.
 */
public class IgfsNewLineRecordResolver extends IgfsByteDelimiterRecordResolver {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Singleton new line resolver. This resolver will resolve records based on new lines
     * regardless if they have '\n' or '\r\n' patterns.
     */
    public static final IgfsNewLineRecordResolver NEW_LINE = new IgfsNewLineRecordResolver(true);

    /** CR symbol. */
    public static final byte SYM_CR = 0x0D;

    /** LF symbol. */
    public static final byte SYM_LF = 0x0A;

    /**
     * Empty constructor required for {@link Externalizable} support.
     */
    public IgfsNewLineRecordResolver() {
        // No-op.
    }

    /**
     * Creates new-line record resolver.
     *
     * @param b Artificial flag to differentiate from empty constructor.
     */
    @SuppressWarnings("UnusedParameters")
    private IgfsNewLineRecordResolver(boolean b) {
        super(new byte[] { SYM_CR, SYM_LF }, new byte[] { SYM_LF });
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsNewLineRecordResolver.class, this);
    }
}
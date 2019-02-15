/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
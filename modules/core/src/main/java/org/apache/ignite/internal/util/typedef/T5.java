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

package org.apache.ignite.internal.util.typedef;

import java.io.Externalizable;
import org.apache.ignite.internal.util.lang.GridFunc;
import org.apache.ignite.internal.util.lang.GridTuple;
import org.apache.ignite.internal.util.lang.GridTuple5;

/**
 * Defines {@code alias} for {@link GridTuple5} by extending it. Since Java doesn't provide type aliases
 * (like Scala, for example) we resort to these types of measures. This is intended to provide for more
 * concise code in cases when readability won't be sacrificed. For more information see {@link GridTuple5}.
 * @see GridFunc
 * @see GridTuple
 */
public class T5<V1, V2, V3, V4, V5> extends GridTuple5<V1, V2, V3, V4, V5> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public T5() {
        // No-op.
    }

    /**
     * Fully initializes this tuple.
     *
     * @param v1 First value.
     * @param v2 Second value.
     * @param v3 Third value.
     * @param v4 Forth value.
     * @param v5 Fifth value.
     */
    public T5(V1 v1, V2 v2, V3 v3, V4 v4, V5 v5) {
        super(v1, v2, v3, v4, v5);
    }
}
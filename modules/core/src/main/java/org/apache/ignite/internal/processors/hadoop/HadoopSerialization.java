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

package org.apache.ignite.internal.processors.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import org.apache.ignite.IgniteCheckedException;
import org.jetbrains.annotations.Nullable;

/**
 * Hadoop serialization. Not thread safe object, must be created for each thread or correctly synchronized.
 */
public interface HadoopSerialization extends AutoCloseable {
    /**
     * Writes the given object to output.
     *
     * @param out Output.
     * @param obj Object to serialize.
     * @throws IgniteCheckedException If failed.
     */
    public void write(DataOutput out, Object obj) throws IgniteCheckedException;

    /**
     * Reads object from the given input optionally reusing given instance.
     *
     * @param in Input.
     * @param obj Object.
     * @return New object or reused instance.
     * @throws IgniteCheckedException If failed.
     */
    public Object read(DataInput in, @Nullable Object obj) throws IgniteCheckedException;

    /**
     * Finalise the internal objects.
     *
     * @throws IgniteCheckedException If failed.
     */
    @Override public void close() throws IgniteCheckedException;
}
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

package org.apache.ignite.ml.dataset.impl.bootstrapping;

import java.util.Arrays;
import java.util.Iterator;
import org.jetbrains.annotations.NotNull;

/**
 * Partition of bootstrapped vectors.
 */
public class BootstrappedDatasetPartition implements AutoCloseable, Iterable<BootstrappedVector> {
    /** Vectors. */
    private final BootstrappedVector[] vectors;

    /**
     * Creates an instance of BootstrappedDatasetPartition.
     *
     * @param vectors Vectors.
     */
    public BootstrappedDatasetPartition(BootstrappedVector[] vectors) {
        this.vectors = vectors;
    }

    /**
     * Returns vector from dataset in according to row id.
     *
     * @param rowId Row id.
     * @return Vector.
     */
    public BootstrappedVector getRow(int rowId) {
        return vectors[rowId];
    }

    /**
     * Returns rows count.
     *
     * @return rows count.
     */
    public int getRowsCount() {
        return vectors.length;
    }

    /** {@inheritDoc} */
    @NotNull @Override public Iterator<BootstrappedVector> iterator() {
        return Arrays.stream(vectors).iterator();
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        //NOP
    }
}

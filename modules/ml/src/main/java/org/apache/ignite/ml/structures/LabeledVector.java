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

package org.apache.ignite.ml.structures;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * Class for vector with label.
 *
 * @param <L> Type of label.
 */
public class LabeledVector<L> extends DatasetRow<Vector> {
    /** Label. */
    private L lb;

    /**
     * Default constructor.
     */
    public LabeledVector() {
        super();
    }

    /**
     * Construct labeled vector.
     *
     * @param vector Vector.
     * @param lb Label.
     */
    public LabeledVector(Vector vector, L lb) {
        super(vector);
        this.lb = lb;
    }

    /**
     * Get the label.
     *
     * @return Label.
     */
    public L label() {
        return lb;
    }

    /**
     * Set the label
     *
     * @param lb Label.
     */
    public void setLabel(L lb) {
        this.lb = lb;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        LabeledVector vector1 = (LabeledVector)o;

        if (vector != null ? !vector.equals(vector1.vector) : vector1.vector != null)
            return false;
        return lb != null ? lb.equals(vector1.lb) : vector1.lb == null;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = vector != null ? vector.hashCode() : 0;
        res = 31 * res + (lb != null ? lb.hashCode() : 0);
        return res;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(vector);
        out.writeObject(lb);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        vector = (Vector)in.readObject();
        lb = (L)in.readObject();
    }
}

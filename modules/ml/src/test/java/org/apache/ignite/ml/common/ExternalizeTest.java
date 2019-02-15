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

package org.apache.ignite.ml.common;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.apache.ignite.ml.math.Destroyable;
import org.apache.ignite.ml.math.distances.EuclideanDistance;
import org.apache.ignite.ml.math.distances.HammingDistance;
import org.apache.ignite.ml.math.distances.ManhattanDistance;
import org.apache.ignite.ml.math.primitives.MathTestConstants;
import org.apache.ignite.ml.math.primitives.matrix.impl.DenseMatrix;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.impl.DelegatingVector;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.apache.ignite.ml.math.primitives.vector.impl.VectorizedViewMatrix;
import org.apache.ignite.ml.structures.Dataset;
import org.apache.ignite.ml.structures.DatasetRow;
import org.apache.ignite.ml.structures.FeatureMetadata;
import org.apache.ignite.ml.structures.LabeledVector;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests for externalizable classes.
 */
public class ExternalizeTest {
    /** */
    @Test
    @SuppressWarnings("unchecked")
    public void test() {
        externalizeTest(new DelegatingVector(new DenseVector(1)));

        externalizeTest(new VectorizedViewMatrix(new DenseMatrix(2, 2), 1, 1, 1, 1));

        externalizeTest(new ManhattanDistance());

        externalizeTest(new HammingDistance());

        externalizeTest(new EuclideanDistance());

        externalizeTest(new FeatureMetadata());

        externalizeTest(new VectorizedViewMatrix(new DenseMatrix(2, 2), 1, 1, 1, 1));

        externalizeTest(new DatasetRow<>(new DenseVector()));

        externalizeTest(new LabeledVector<>(new DenseVector(), null));

        externalizeTest(new Dataset<DatasetRow<Vector>>(new DatasetRow[] {}, new FeatureMetadata[] {}));
    }

    /** */
    @SuppressWarnings("unchecked")
    private <T> void externalizeTest(T initObj) {
        T objRestored = null;

        try {
            ByteArrayOutputStream byteArrOutputStream = new ByteArrayOutputStream();
            ObjectOutputStream objOutputStream = new ObjectOutputStream(byteArrOutputStream);

            objOutputStream.writeObject(initObj);

            ByteArrayInputStream byteArrInputStream = new ByteArrayInputStream(byteArrOutputStream.toByteArray());
            ObjectInputStream objInputStream = new ObjectInputStream(byteArrInputStream);

            objRestored = (T)objInputStream.readObject();

            assertEquals(MathTestConstants.VAL_NOT_EQUALS, initObj, objRestored);

           assertEquals(MathTestConstants.VAL_NOT_EQUALS, 0, Integer.compare(initObj.hashCode(), objRestored.hashCode()));
        }
        catch (ClassNotFoundException | IOException e) {
            fail(e + " [" + e.getMessage() + "]");
        }
        finally {
            if (objRestored instanceof Destroyable)
                ((Destroyable)objRestored).destroy();
        }
    }
}

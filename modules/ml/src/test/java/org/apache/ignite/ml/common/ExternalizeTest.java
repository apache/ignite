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

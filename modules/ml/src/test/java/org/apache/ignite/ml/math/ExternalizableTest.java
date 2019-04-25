/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.ml.math;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.apache.ignite.ml.math.primitives.MathTestConstants;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Common test for externalization.
 */
public interface ExternalizableTest<T extends Externalizable> {
    /** */
    @SuppressWarnings("unchecked")
    public default void externalizeTest(T initObj) {
        T objRestored = null;

        try {
            ByteArrayOutputStream byteArrOutputStream = new ByteArrayOutputStream();
            ObjectOutputStream objOutputStream = new ObjectOutputStream(byteArrOutputStream);

            objOutputStream.writeObject(initObj);

            ByteArrayInputStream byteArrInputStream = new ByteArrayInputStream(byteArrOutputStream.toByteArray());
            ObjectInputStream objInputStream = new ObjectInputStream(byteArrInputStream);

            objRestored = (T)objInputStream.readObject();

            assertTrue(MathTestConstants.VAL_NOT_EQUALS, initObj.equals(objRestored));
            assertTrue(MathTestConstants.VAL_NOT_EQUALS, Integer.compare(initObj.hashCode(), objRestored.hashCode()) == 0);
        }
        catch (ClassNotFoundException | IOException e) {
            fail(e + " [" + e.getMessage() + "]");
        }
        finally {
            if (objRestored != null && objRestored instanceof Destroyable)
                ((Destroyable)objRestored).destroy();
        }
    }

    /** */
    @Test
    public void testExternalization();
}

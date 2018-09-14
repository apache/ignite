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
 * TODO: IGNITE-7325 remove this class from all test and change on ExternalizableTest
 */
public abstract class ExternalizeTest<T extends Externalizable & Destroyable> {
    /** */
    @SuppressWarnings("unchecked")
    protected void externalizeTest(T initObj) {
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
            if (objRestored != null)
                objRestored.destroy();
        }
    }

    /** */
    @Test
    public abstract void externalizeTest();
}

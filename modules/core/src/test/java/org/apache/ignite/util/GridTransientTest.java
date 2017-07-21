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

package org.apache.ignite.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 * Transient value serialization test.
 */
@GridCommonTest(group = "Utils")
public class GridTransientTest extends GridCommonAbstractTest implements Serializable {
    /** */
    private static final String VALUE = "value";

    /** */
    @SuppressWarnings({"TransientFieldNotInitialized"})
    private final transient String data1;

    /** */
    @SuppressWarnings({"TransientFieldNotInitialized"})
    private final transient String data2 = VALUE;

    /** */
    public GridTransientTest() {
        data1 = VALUE;
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransientSerialization() throws Exception {
        GridTransientTest objSrc = new GridTransientTest();

        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();

        ObjectOutputStream objOut = new ObjectOutputStream(byteOut);

        objOut.writeObject(objSrc);

        objOut.close();

        ObjectInputStream objIn = new ObjectInputStream(new ByteArrayInputStream(byteOut.toByteArray()));

        GridTransientTest objDest = (GridTransientTest)objIn.readObject();

        System.out.println("Data1 value: " + objDest.data1);
        System.out.println("Data2 value: " + objDest.data2);

        assertEquals(objDest.data1, null);
        assertEquals(objDest.data2, VALUE);
    }
}
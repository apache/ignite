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

package org.apache.ignite.internal.schema.testobjects;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;
import java.util.Random;
import org.apache.ignite.internal.testframework.IgniteTestUtils;

/**
 * Test object.
 */
@SuppressWarnings("InstanceVariableMayNotBeInitialized")
public class TestSimpleObject implements Serializable {
    /**
     * Creates an object with random data.
     */
    public static TestSimpleObject randomObject(Random rnd) {
        TestSimpleObject obj = new TestSimpleObject();

        obj.longCol = rnd.nextLong();
        obj.stringCol = IgniteTestUtils.randomString(rnd, 255);

        return obj;
    }

    Long longCol;

    Integer intCol;

    byte[] bytesCol;

    String stringCol;

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TestSimpleObject object = (TestSimpleObject) o;

        return Objects.equals(longCol, object.longCol)
                && Objects.equals(intCol, object.intCol)
                && Arrays.equals(bytesCol, object.bytesCol)
                && Objects.equals(stringCol, object.stringCol);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return 42;
    }
}

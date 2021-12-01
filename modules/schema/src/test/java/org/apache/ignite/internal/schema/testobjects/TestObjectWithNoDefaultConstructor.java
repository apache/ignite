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

import java.util.Objects;
import java.util.Random;

/**
 * Test object without default constructor.
 */
public class TestObjectWithNoDefaultConstructor {
    /**
     * Creates an object with random data.
     */
    public static TestObjectWithNoDefaultConstructor randomObject(Random rnd) {
        return new TestObjectWithNoDefaultConstructor(rnd.nextLong(), rnd.nextInt());
    }

    private final long primLongCol;

    private final int primIntCol;

    /**
     * Private constructor.
     */
    public TestObjectWithNoDefaultConstructor(long longVal, int intVal) {
        primLongCol = longVal;
        primIntCol = intVal;
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TestObjectWithNoDefaultConstructor object = (TestObjectWithNoDefaultConstructor) o;

        return primLongCol == object.primLongCol;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(primLongCol);
    }
}

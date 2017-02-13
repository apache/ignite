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

package org.apache.ignite.math.impls;

import org.apache.ignite.math.Vector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.sun.tools.internal.ws.processor.generator.GeneratorConstants.IS;
import static org.junit.Assert.*;

/**
 * Unit tests for {@link DenseLocalOffHeapVector}.
 */
public class DenseLocalOffHeapVectorTest {

    /** */
    private DenseLocalOffHeapVector offHeapVector;

    /** */
    @Before
    public void setup(){
        offHeapVector = new DenseLocalOffHeapVector(MathTestConstants.STORAGE_SIZE);
    }

    /** */
    @After
    public void tearDown(){
        offHeapVector.destroy();
    }

    /** */
    @Test
    public void copy() throws Exception {
        Vector copy = offHeapVector.copy();

        try {
            assertTrue(MathTestConstants.VALUE_NOT_EQUALS, offHeapVector.equals(copy));
        } finally {
            copy.destroy();
        }
    }

    /** */
    @Test
    public void like() throws Exception {
        Vector like = offHeapVector.like(0);

        try {
            assertTrue(MathTestConstants.UNEXPECTED_VALUE, like.getClass() == DenseLocalOffHeapVector.class);

            like.destroy();

            like = offHeapVector.like(MathTestConstants.STORAGE_SIZE);

            assertTrue(MathTestConstants.UNEXPECTED_VALUE, like.getClass() == DenseLocalOffHeapVector.class);
        } finally {
            like.destroy();
        }
    }

    /** */
    @Test
    public void likeMatrix() throws Exception {
        // TODO impl
    }

}
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

package org.apache.ignite.math;

import org.apache.ignite.math.impls.*;
import org.junit.*;
import java.io.*;

/**
 * TODO: add description.
 */
public class TracerTest {
    /**
     *
     * @param size
     * @return
     */
    private Vector makeRandomVector(int size) {
        DenseLocalOnHeapVector vec = new DenseLocalOnHeapVector(size);

        vec.assign((idx) -> Math.random());

        return vec;
    }

    /**
     *
     */
    @Test
    public void testAsciiVectorTracer() {
        Vector vec = makeRandomVector(20);

        Tracer.showAscii(vec);
        Tracer.showAscii(vec, "%2f");
        Tracer.showAscii(vec, "%.3g");
    }

    /**
     *
     */
    @Test
    public void testHtmlVectorTracer() throws IOException {
        Vector vec = makeRandomVector(20);

        Tracer.showHtml(vec);
    }
}

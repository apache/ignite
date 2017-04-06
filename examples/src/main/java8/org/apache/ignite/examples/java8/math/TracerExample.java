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

package org.apache.ignite.examples.java8.math;

import java.awt.Color;
import java.io.IOException;
import org.apache.ignite.math.Tracer;
import org.apache.ignite.math.impls.matrix.DenseLocalOnHeapMatrix;

/** */
public class TracerExample {
    /** */
    private static final Tracer.ColorMapper COLOR_MAPPER = d -> {
        if (d <= 0.33)
            return Color.RED;
        else if (d <= 0.66)
            return Color.GREEN;
        else
            return Color.BLUE;
    };

    /** */
    public static void main(String[] args) throws IOException {
        // Tracer is a simple utility class that allows pretty-printing of matrices/vectors
        DenseLocalOnHeapMatrix m = new DenseLocalOnHeapMatrix(new double[][] {
            {1.12345, 2.12345},
            {3.12345, 4.12345}
        });

        Tracer.showAscii(m, "%.3g");
        Tracer.showHtml(m, COLOR_MAPPER);
    }
}

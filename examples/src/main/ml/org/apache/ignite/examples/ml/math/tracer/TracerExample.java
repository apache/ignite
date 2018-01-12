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

package org.apache.ignite.examples.ml.math.tracer;

import java.awt.Color;
import java.io.IOException;
import org.apache.ignite.ml.math.Tracer;
import org.apache.ignite.ml.math.impls.matrix.DenseLocalOnHeapMatrix;

/**
 * Example of using {@link Tracer} utility API.
 */
public class TracerExample {
    /**
     * Double to color mapper example.
     */
    private static final Tracer.ColorMapper COLOR_MAPPER = d -> {
        if (d <= 1.5)
            return Color.RED;
        else if (d <= 2.5)
            return Color.GREEN;
        else
            return Color.BLUE;
    };

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String[] args) throws IOException {
        System.out.println(">>> Tracer utility example started.");

        // Tracer is a simple utility class that allows pretty-printing of matrices/vectors.
        DenseLocalOnHeapMatrix m = new DenseLocalOnHeapMatrix(new double[][] {
            {1.12345, 2.12345},
            {3.12345, 4.12345}
        });

        System.out.println("\n>>> Tracer output to console in ASCII.");
        Tracer.showAscii(m, "%.3g");

        System.out.println("\n>>> Tracer output to browser in HTML.");
        Tracer.showHtml(m, COLOR_MAPPER, true);

        System.out.println("\n>>> Tracer utility example completed.");
    }
}

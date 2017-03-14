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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.List;
import org.apache.ignite.math.impls.MathTestConstants;
import org.apache.ignite.math.impls.matrix.DenseLocalOnHeapMatrix;
import org.apache.ignite.math.impls.vector.*;
import org.junit.*;
import java.awt.*;
import java.io.*;

import static java.nio.file.Files.createTempFile;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link Tracer}.
 */
public class TracerTest {
    private static final String DEFAULT_FORMAT = "%.10f";
    private static final double DEFAULT_DELTA = 0.000000001d;

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
        Vector vec1 = makeRandomVector(1000);

        // Default color mapping.
        Tracer.showHtml(vec1);

        // Custom color mapping.
        Tracer.showHtml(vec1, new Tracer.ColorMapper() {
            @Override
            public Color apply(Double d) {
                if (d <= 0.33)
                    return Color.RED;
                else if (d <= 0.66)
                    return Color.GREEN;
                else
                    return Color.BLUE;
            }
        });

        // Default color mapping with sorted vector.
        Tracer.showHtml(vec1.sort());
    }

    /** */
    @Test
    public void testWriteVecToCSVFile() throws IOException {
        DenseLocalOnHeapVector vector = new DenseLocalOnHeapVector(MathTestConstants.STORAGE_SIZE);

        for(int i = 0; i < vector.size(); i++)
            vector.set(i,Math.random());

        Path file = createTempFile("DenseLocalOnHeapVector", ".csv");
        Tracer.saveAsCsv(vector, DEFAULT_FORMAT,file.toString());

        List<String> strings = Files.readAllLines(file);
        Optional<String> reduce = strings.stream().reduce((s1, s2) -> s1 + s2);
        String[] csvVals = reduce.get().split(",");

        for (int i = 0; i < vector.size(); i++) {
            Double csvVal = Double.valueOf(csvVals[i]);

            assertEquals("Unexpected value.", csvVal, vector.get(i), DEFAULT_DELTA);
        }

        Files.deleteIfExists(file);
    }

    /** */
    @Test
    public void testWriteMtxToCSVFile() throws IOException {
        DenseLocalOnHeapMatrix matrix = new DenseLocalOnHeapMatrix(MathTestConstants.STORAGE_SIZE, MathTestConstants.STORAGE_SIZE);

        for(int i = 0; i < matrix.rowSize(); i++)
            for(int j = 0; j < matrix.columnSize(); j++)
                matrix.set(i, j, Math.random());


        Path file = createTempFile("DenseLocalOnHeapVector", ".csv");
        Tracer.saveAsCsv(matrix, DEFAULT_FORMAT,file.toString());

        List<String> strings = Files.readAllLines(file);
        Optional<String> reduce = strings.stream().reduce((s1, s2) -> s1 + s2);
        String[] csvVals = reduce.get().split(",");

        for(int i = 0; i < matrix.rowSize(); i++)
            for(int j = 0; j < matrix.columnSize(); j++) {
                Double csvVal = Double.valueOf(csvVals[i * matrix.rowSize() + j]);

                assertEquals("Unexpected value.", csvVal, matrix.get(i, j), DEFAULT_DELTA);
            }
    }
}

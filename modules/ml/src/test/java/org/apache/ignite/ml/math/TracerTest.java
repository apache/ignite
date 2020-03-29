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

import java.awt.Color;
import java.awt.Desktop;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import org.apache.ignite.ml.math.primitives.MathTestConstants;
import org.apache.ignite.ml.math.primitives.matrix.Matrix;
import org.apache.ignite.ml.math.primitives.matrix.impl.DenseMatrix;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.junit.Test;

import static java.nio.file.Files.createTempFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests for {@link Tracer}.
 */
public class TracerTest {
    /** */
    private static final String DEFAULT_FORMAT = "%.10f";

    /** */
    private static final double DEFAULT_DELTA = 0.000000001d;

    /**
     * Color mapper that maps [0, 1] range into three distinct RGB segments.
     */
    private static final Tracer.ColorMapper COLOR_MAPPER = new Tracer.ColorMapper() {
        /** {@inheritDoc} */
        @Override public Color apply(Double d) {
            if (d <= 0.33)
                return Color.RED;
            else if (d <= 0.66)
                return Color.GREEN;
            else
                return Color.BLUE;
        }
    };

    /**
     * @param size Vector size.
     */
    private Vector makeRandomVector(int size) {
        DenseVector vec = new DenseVector(size);

        vec.assign((idx) -> Math.random());

        return vec;
    }

    /**
     * @param rows Amount of rows in matrix.
     * @param cols Amount of columns in matrix.
     */
    private Matrix makeRandomMatrix(int rows, int cols) {
        DenseMatrix mtx = new DenseMatrix(rows, cols);

        // Missing assign(f)?
        mtx.map((d) -> Math.random());

        return mtx;
    }

    /** */
    @Test
    public void testAsciiVectorTracer() {
        Vector vec = makeRandomVector(20);

        Tracer.showAscii(vec);
        Tracer.showAscii(vec, "%2f");
        Tracer.showAscii(vec, "%.3g");
    }

    /** */
    @Test
    public void testAsciiMatrixTracer() {
        Matrix mtx = makeRandomMatrix(10, 10);

        Tracer.showAscii(mtx);
        Tracer.showAscii(mtx, "%2f");
        Tracer.showAscii(mtx, "%.3g");
    }

    /** */
    @Test
    public void testHtmlVectorTracer() throws IOException {
        Vector vec1 = makeRandomVector(1000);

        // Default color mapping.
        verifyShowHtml(() -> Tracer.showHtml(vec1));

        // Custom color mapping.
        verifyShowHtml(() -> Tracer.showHtml(vec1, COLOR_MAPPER));

        // Default color mapping with sorted vector.
        verifyShowHtml(() -> Tracer.showHtml(vec1.copy().sort()));
    }

    /** */
    @Test
    public void testHtmlMatrixTracer() throws IOException {
        Matrix mtx1 = makeRandomMatrix(100, 100);

        // Custom color mapping.
        verifyShowHtml(() -> Tracer.showHtml(mtx1, COLOR_MAPPER));

        Matrix mtx2 = new DenseMatrix(100, 100);

        double MAX = (double)(mtx2.rowSize() * mtx2.columnSize());

        mtx2.assign((x, y) -> (double)(x * y) / MAX);

        verifyShowHtml(() -> Tracer.showHtml(mtx2));
    }

    /** */
    @Test
    public void testHtmlVectorTracerWithAsciiFallback() throws IOException {
        Vector vec1 = makeRandomVector(1000);

        // Default color mapping.
        Tracer.showHtml(vec1, true);

        // Custom color mapping.
        Tracer.showHtml(vec1, COLOR_MAPPER, true);

        // Default color mapping with sorted vector.
        Tracer.showHtml(vec1.copy().sort(), true);
    }

    /** */
    @Test
    public void testHtmlMatrixTracerWithAsciiFallback() throws IOException {
        Matrix mtx1 = makeRandomMatrix(100, 100);

        // Custom color mapping.
        Tracer.showHtml(mtx1, COLOR_MAPPER, true);

        Matrix mtx2 = new DenseMatrix(100, 100);

        double MAX = (double)(mtx2.rowSize() * mtx2.columnSize());

        mtx2.assign((x, y) -> (double)(x * y) / MAX);

        Tracer.showHtml(mtx2, true);
    }

    /** */
    @Test
    public void testWriteVectorToCSVFile() throws IOException {
        DenseVector vector = new DenseVector(MathTestConstants.STORAGE_SIZE);

        for (int i = 0; i < vector.size(); i++)
            vector.set(i, Math.random());

        Path file = createTempFile("vector", ".csv");

        Tracer.saveAsCsv(vector, DEFAULT_FORMAT, file.toString());

        System.out.println("Vector exported: " + file.getFileName());

        List<String> strings = Files.readAllLines(file);
        Optional<String> reduce = strings.stream().reduce((s1, s2) -> s1 + s2);
        String[] csvVals = reduce.orElse("").split(",");

        for (int i = 0; i < vector.size(); i++) {
            Double csvVal = Double.valueOf(csvVals[i]);

            assertEquals("Unexpected value.", csvVal, vector.get(i), DEFAULT_DELTA);
        }

        Files.deleteIfExists(file);
    }

    /** */
    @Test
    public void testWriteMatrixToCSVFile() throws IOException {
        DenseMatrix matrix = new DenseMatrix(MathTestConstants.STORAGE_SIZE, MathTestConstants.STORAGE_SIZE);

        for (int i = 0; i < matrix.rowSize(); i++)
            for (int j = 0; j < matrix.columnSize(); j++)
                matrix.set(i, j, Math.random());

        Path file = createTempFile("matrix", ".csv");

        Tracer.saveAsCsv(matrix, DEFAULT_FORMAT, file.toString());

        System.out.println("Matrix exported: " + file.getFileName());

        List<String> strings = Files.readAllLines(file);
        Optional<String> reduce = strings.stream().reduce((s1, s2) -> s1 + s2);
        String[] csvVals = reduce.orElse("").split(",");

        for (int i = 0; i < matrix.rowSize(); i++)
            for (int j = 0; j < matrix.columnSize(); j++) {
                Double csvVal = Double.valueOf(csvVals[i * matrix.rowSize() + j]);

                assertEquals("Unexpected value.", csvVal, matrix.get(i, j), DEFAULT_DELTA);
            }

        Files.deleteIfExists(file);
    }

    /** */
    private void verifyShowHtml(ShowHtml code) throws IOException {
        final boolean browseSupported = Desktop.isDesktopSupported()
            && Desktop.getDesktop().isSupported(Desktop.Action.BROWSE);

        try {
            code.showHtml();
            if (!browseSupported)
                fail("Expected exception was not caught: " + UnsupportedOperationException.class.getSimpleName());
        }
        catch (UnsupportedOperationException uoe) {
            if (browseSupported)
                throw uoe;
        }
    }

    /** */
    @FunctionalInterface private interface ShowHtml {
        /** */
        void showHtml() throws IOException;
    }
}

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

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import java.awt.*;
import java.io.*;
import java.util.*;
import java.util.function.*;
import java.util.stream.*;

/**
 * This object allows to display matrices and vectors using GUI.
 */
public class Tracer {
    /**
     * Double to color mapper.
     */
    interface ColorMapper extends Function<Double, Color> {}

    // Continues red-to-blue color mapping.
    static private ColorMapper defaultColorMapper(double min, double max) {
        double range = max - min;

        return new ColorMapper() {
            @Override
            public Color apply(Double d) {
                int r = (int)Math.round(255 * d);
                int g = 0;
                int b = (int)Math.round(255 * (1 - d));

                return new Color(r, g, b);
            }
        };
    }

    // Default vector color mapper implementation that map given double value
    // to continues red-blue (R_B) specter.
    static private ColorMapper mkVectorColorMapper(Vector vec) {
        return defaultColorMapper(vec.minValue().get(), vec.maxValue().get());
    }

    // Default matrix color mapper implementation that map given double value
    // to continues red-blue (R_B) specter.
    static private ColorMapper mkMatrixColorMapper(Matrix mtx) {
        return defaultColorMapper(mtx.minValue().get(), mtx.maxValue().get());
    }

    /**
     * 
     * @param vec
     */
    static void showAscii(Vector vec, IgniteLogger log, String fmt) {
        String cls = vec.getClass().getSimpleName();

        log.info(String.format("%s(%d) [%s]", cls, vec.size(), mkString(vec, fmt) + "]"));
    }

    /**
     *
     * @param vec
     */
    static void showAscii(Vector vec, IgniteLogger log) {
        showAscii(vec, log, "%4f");
    }

    /**
     *
     * @param vec
     */
    static void showAscii(Vector vec, String fmt) {
        String cls = vec.getClass().getSimpleName();

        System.out.println(String.format("%s(%d) [%s]", cls, vec.size(), mkString(vec, fmt) + "]"));
    }

    /**
     *
     * @param vec
     */
    static void showAscii(Vector vec) {
        showAscii(vec, "%4f");
    }

    /**
     * Saves given vector as CSV file.
     *
     * @param vec Vector to save.
     * @param fmt Format to use.
     * @param filePath Path of the file to save to.
     */
    static void saveAsCsv(Vector vec, String fmt, String filePath) {
        // TODO.
    }

    /**
     * Saves given matrix as CSV file.
     *
     * @param mtx Matrix to save.
     * @param fmt Format to use.
     * @param filePath Path of the file to save to.
     */
    static void saveAsCsv(Matrix mtx, String fmt, String filePath) {
        // TODO.
    }

    /**
     * Shows given matrix in the browser with D3-based visualization.
     *
     * @param mtx Matrix to show.
     * @throws IOException Thrown in case of any errors.
     */
    static void showHtml(Matrix mtx) throws IOException {
        showHtml(mtx, mkMatrixColorMapper(mtx));
    }

    /**
     * Shows given matrix in the browser with D3-based visualization.
     *
     * @param mtx Matrix to show.
     * @param cm Optional color mapper. If not provided - red-to-blue (R_B) mapper will be used.
     * @throws IOException Thrown in case of any errors.
     */
    static void showHtml(Matrix mtx, ColorMapper cm) throws IOException {
        // Read it every time so that we can change it at runtime.
        String tmpl = fileToString("d3-matrix-template.html");

        // TODO: update template.

        openHtmlFile(tmpl);
    }

    /**
     * Shows given vector in the browser with D3-based visualization.
     *
     * @param vec Vector to show.
     * @throws IOException Thrown in case of any errors.
     */
    static void showHtml(Vector vec) throws IOException {
        showHtml(vec, mkVectorColorMapper(vec));
    }

    /**
     *
     * @param d
     * @param clr
     * @return
     */
    static private String dataColorJson(double d, Color clr) {
        return "{" +
            "d: " + String.format("%4f", d) +
            ", r: " + clr.getRed() +
            ", g: " + clr.getGreen() +
            ", b: " + clr.getBlue() +
            "}";
    }

    /**
     * Shows given vector in the browser with D3-based visualization.
     *
     * @param vec Vector to show.
     * @param cm Optional color mapper. If not provided - red-to-blue (R_B) mapper will be used.
     * @throws IOException Thrown in case of any errors.
     */
    static void showHtml(Vector vec, ColorMapper cm) throws IOException {
        // Read it every time so that we can change it at runtime.
        String tmpl = fileToString("d3-vector-template.html");

        String cls = vec.getClass().getSimpleName();

        double min = vec.minValue().get();
        double max = vec.maxValue().get();

        openHtmlFile(tmpl.
            replaceAll("/\\*@NAME@\\*/.*\n", "var name = \"" + cls + "\";\n").
            replaceAll("/\\*@MIN@\\*/.*\n", "var min = " + dataColorJson(min, cm.apply(min))+ ";\n").
            replaceAll("/\\*@MAX@\\*/.*\n", "var max = " + dataColorJson(max, cm.apply(max))+ ";\n").
            replaceAll("/\\*@DATA@\\*/.*\n", "var data = [" + mkJsonString(vec, cm) + "];\n")
        );
    }

    /**
     * Reads file content into the string.
     *
     * @param fileName Name of the file (on classpath) to read.
     * @return Content of the file.
     * @throws IOException
     */
    private static String fileToString(String fileName) throws IOException {
        InputStreamReader is = new InputStreamReader(Tracer.class.getResourceAsStream(fileName));

        String str = new BufferedReader(is).lines().collect(Collectors.joining("\n"));

        is.close();

        return str;
    }

    /**
     * Opens file in the browser with given HTML content.
     *
     * @param html HTML content.
     * @throws IOException Thrown in case of any errors.
     */
    static private void openHtmlFile(String html) throws IOException {
        File temp = File.createTempFile(IgniteUuid.randomUuid().toString(), ".html");

        BufferedWriter bw = new BufferedWriter(new FileWriter(temp));

        bw.write(html);

        bw.close();

        Desktop.getDesktop().browse(temp.toURI());
    }

    /**
     * Gets string presentation of this vector.
     *
     * @param vec Vector to string-ify.
     * @param fmt {@link String#format(Locale, String, Object...)} format.
     * @return
     */
    static String mkString(Vector vec, String fmt) {
        boolean first = true;

        StringBuilder buf = new StringBuilder();

        for (Vector.Element x : vec.all()) {
            String s = String.format(fmt, x.get());

            if (!first) {
                buf.append(", ");
                buf.append(s);
            }
            else {
                buf.append(s);
                first = false;
            }
        }

        return buf.toString();
    }

    /**
     * Gets JSON string presentation of this vector.
     *
     * @param vec Vector to string-ify.
     * @param cm Color mapper to user.
     * @return
     */
    static String mkJsonString(Vector vec, ColorMapper cm) {
        boolean first = true;

        StringBuilder buf = new StringBuilder();

        for (Vector.Element x : vec.all()) {
            double d = x.get();

            String s = dataColorJson(d, cm.apply(d));

            if (!first)
                buf.append(", ");

            buf.append(s);
            first = false;
        }

        return buf.toString();
    }

    /**
     *
     * @param mtx
     * @param fmt
     * @return
     */
    static String mkString(Matrix mtx, String fmt) {
        boolean first = true;

        StringBuffer buf = new StringBuffer();

        // TODO

        return buf.toString();
    }
}

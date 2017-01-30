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
import java.awt.*;

/**
 * This object allows to display matrices and vectors using GUI.
 */
public class Tracer {
    public interface MatrixColorPredicate {
        /**
         *
         * @param x Matrix row index.
         * @param y Matrix column index.
         * @param v Matrix value at {@code (x, y)}.
         */
        boolean apply(int x, int y, double v);
    }

    /**
     * Color selector is a combination of a predicate and the color to use when
     * that predicate returns true for a given matrix element.
     */
    public class MatrixColorSelector {
        private MatrixColorPredicate pred;
        private Color clr;

        /**
         *
         * @param pred Color predicate.
         * @param clr Color to use.
         */
        public MatrixColorSelector(MatrixColorPredicate pred, Color clr) {
            this.pred = pred;
            this.clr = clr;
        }

        /**
         *
         * @param clr Color to use.
         */
        public MatrixColorSelector(Color clr) {
            this.pred = (int x, int y, double v) -> true;
            this.clr = clr;
        }

        /**
         *
         * @return
         */
        public MatrixColorPredicate getPredicate() {
            return pred;
        }

        /**
         *
         * @return
         */
        public Color getColor() {
            return clr;
        }
    }

    /**
     *
     */
    public class MatrixTracer {
        private Matrix mtx;
        private MatrixColorSelector[] selectors;

        /**
         * Creates matrix tracer with given matrix and set of color selectors.
         *
         * @param mtx Matrix.
         * @param selectors Set of color selector (one or more).
         */
        public MatrixTracer(Matrix mtx, MatrixColorSelector... selectors) {
            assert selectors.length > 0;
            
            this.mtx = mtx;
            this.selectors = selectors;
        }

        /**
         * Creates matrix tracer with given matrix and single color for all values.
         *
         * @param mtx Matrix.
         * @param clr Color to use for all matrix values.
         */
        public MatrixTracer(Matrix mtx, Color clr) {
            assert selectors.length > 0;

            this.mtx = mtx;
            this.selectors = new MatrixColorSelector[] {
                new MatrixColorSelector((int x, int y, double v) -> true, clr)
            };
        }

        /**
         *
         * @return
         */
        public Matrix getMatrix() {
            return mtx;
        }

        /**
         * 
         * @return
         */
        public MatrixColorSelector[] getSelectors() {
            return selectors;
        }
    }

    /**
     * Shows one or more given matrices of the same cardinality.
     *
     * @param bgCol Background color.
     * @param tracers Matrix tracers (one or more).
     */
    static void showMatrices(Color bgCol, MatrixTracer... tracers) {
        assert tracers.length > 0;

        // TODO.
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
     *
     * @param vec
     * @param fmt
     * @return
     */
    static String mkString(Vector vec, String fmt) {
        boolean first = true;

        StringBuffer buf = new StringBuffer();

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
}

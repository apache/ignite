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

package org.apache.ignite.ml.math.decompositions;

import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.functions.Functions;

/**
 * This class provides EigenDecomposition of given matrix. The class is based on
 * class with similar name from <a href="http://mahout.apache.org/">Apache Mahout</a> library.
 * <p>
 * @see <a href=http://mathworld.wolfram.com/EigenDecomposition.html>MathWorld</a></p>
 */
public class EigenDecomposition extends DecompositionSupport {
    /** Row and column dimension (square matrix). */
    private final int n;

    /** Array for internal storage of eigen vectors. */
    private final Matrix v;

    /** Array for internal storage of eigenvalues. */
    private final Vector d;
    /** Array for internal storage of eigenvalues. */
    private final Vector e;

    /**
     * @param matrix Matrix to decompose.
     */
    public EigenDecomposition(Matrix matrix) {
        this(matrix, isSymmetric(matrix));
    }

    /**
     * @param matrix Matrix to decompose.
     * @param isSymmetric {@code true} if matrix passes symmetry check, {@code false otherwise}.
     */
    public EigenDecomposition(Matrix matrix, boolean isSymmetric) {
        n = matrix.columnSize();

        d = likeVector(matrix);
        e = likeVector(matrix);
        v = like(matrix);

        if (isSymmetric) {
            v.assign(matrix);

            // Tridiagonalize.
            tred2();

            // Diagonalize.
            tql2();

        }
        else
            // Reduce to Hessenberg form.
            // Reduce Hessenberg to real Schur form.
            hqr2(orthes(matrix));
    }

    /**
     * Return the eigen vector matrix.
     *
     * @return V
     */
    public Matrix getV() {
        return like(v).assign(v);
    }

    /**
     * Return the real parts of the eigenvalues
     */
    public Vector getRealEigenValues() {
        return d;
    }

    /**
     * Return the imaginary parts of the eigenvalues.
     *
     * @return Vector of imaginary parts.
     */
    public Vector getImagEigenvalues() {
        return e;
    }

    /**
     * Return the block diagonal eigenvalue matrix
     *
     * @return D
     */
    public Matrix getD() {
        Matrix res = like(v, d.size(), d.size());
        res.assign(0);
        res.viewDiagonal().assign(d);
        for (int i = 0; i < n; i++) {
            double v = e.getX(i);
            if (v > 0)
                res.setX(i, i + 1, v);
            else if (v < 0)
                res.setX(i, i - 1, v);
        }
        return res;
    }

    /**
     * Destroys decomposition components and other internal components of decomposition.
     */
    @Override public void destroy() {
        e.destroy();
        v.destroy();
        d.destroy();
    }

    /** */
    private void tred2() {
        //  This is derived from the Algol procedures tred2 by
        //  Bowdler, Martin, Reinsch, and Wilkinson, Handbook for
        //  Auto. Comp., Vol.ii-Linear Algebra, and the corresponding
        //  Fortran subroutine in EISPACK.

        d.assign(v.viewColumn(n - 1));

        // Householder reduction to tridiagonal form.

        for (int i = n - 1; i > 0; i--) {

            // Scale to avoid under/overflow.
            double scale = d.viewPart(0, i).kNorm(1);
            double h = 0.0;

            if (scale == 0.0) {
                e.setX(i, d.getX(i - 1));
                for (int j = 0; j < i; j++) {
                    d.setX(j, v.getX(i - 1, j));
                    v.setX(i, j, 0.0);
                    v.setX(j, i, 0.0);
                }
            }
            else {

                // Generate Householder vector.

                for (int k = 0; k < i; k++) {
                    d.setX(k, d.getX(k) / scale);
                    h += d.getX(k) * d.getX(k);
                }

                double f = d.getX(i - 1);
                double g = Math.sqrt(h);

                if (f > 0)
                    g = -g;

                e.setX(i, scale * g);
                h -= f * g;
                d.setX(i - 1, f - g);

                for (int j = 0; j < i; j++)
                    e.setX(j, 0.0);

                // Apply similarity transformation to remaining columns.

                for (int j = 0; j < i; j++) {
                    f = d.getX(j);
                    v.setX(j, i, f);
                    g = e.getX(j) + v.getX(j, j) * f;

                    for (int k = j + 1; k <= i - 1; k++) {
                        g += v.getX(k, j) * d.getX(k);
                        e.setX(k, e.getX(k) + v.getX(k, j) * f);
                    }

                    e.setX(j, g);
                }

                f = 0.0;

                for (int j = 0; j < i; j++) {
                    e.setX(j, e.getX(j) / h);
                    f += e.getX(j) * d.getX(j);
                }

                double hh = f / (h + h);

                for (int j = 0; j < i; j++)
                    e.setX(j, e.getX(j) - hh * d.getX(j));

                for (int j = 0; j < i; j++) {
                    f = d.getX(j);
                    g = e.getX(j);

                    for (int k = j; k <= i - 1; k++)
                        v.setX(k, j, v.getX(k, j) - (f * e.getX(k) + g * d.getX(k)));

                    d.setX(j, v.getX(i - 1, j));
                    v.setX(i, j, 0.0);
                }
            }

            d.setX(i, h);
        }
    }

    /** */
    private Matrix orthes(Matrix matrix) {
        // Working storage for nonsymmetric algorithm.
        Vector ort = likeVector(matrix);
        Matrix hessenBerg = like(matrix).assign(matrix);

        //  This is derived from the Algol procedures orthes and ortran,
        //  by Martin and Wilkinson, Handbook for Auto. Comp.,
        //  Vol.ii-Linear Algebra, and the corresponding
        //  Fortran subroutines in EISPACK.

        int low = 0;
        int high = n - 1;

        for (int m = low + 1; m <= high - 1; m++) {

            // Scale column.

            Vector hCol = hessenBerg.viewColumn(m - 1).viewPart(m, high - m + 1);
            double scale = hCol.kNorm(1);

            if (scale != 0.0) {
                // Compute Householder transformation.
                ort.viewPart(m, high - m + 1).map(hCol, Functions.plusMult(1 / scale));
                double h = ort.viewPart(m, high - m + 1).getLengthSquared();

                double g = Math.sqrt(h);

                if (ort.getX(m) > 0)
                    g = -g;

                h -= ort.getX(m) * g;
                ort.setX(m, ort.getX(m) - g);

                // Apply Householder similarity transformation
                // H = (I-u*u'/h)*H*(I-u*u')/h)

                Vector ortPiece = ort.viewPart(m, high - m + 1);

                for (int j = m; j < n; j++) {
                    double f = ortPiece.dot(hessenBerg.viewColumn(j).viewPart(m, high - m + 1)) / h;
                    hessenBerg.viewColumn(j).viewPart(m, high - m + 1).map(ortPiece, Functions.plusMult(-f));
                }

                for (int i = 0; i <= high; i++) {
                    double f = ortPiece.dot(hessenBerg.viewRow(i).viewPart(m, high - m + 1)) / h;
                    hessenBerg.viewRow(i).viewPart(m, high - m + 1).map(ortPiece, Functions.plusMult(-f));
                }

                ort.setX(m, scale * ort.getX(m));
                hessenBerg.setX(m, m - 1, scale * g);
            }
        }

        // Accumulate transformations (Algol's ortran).

        v.assign(0);
        v.viewDiagonal().assign(1);

        for (int m = high - 1; m >= low + 1; m--) {
            if (hessenBerg.getX(m, m - 1) != 0.0) {
                ort.viewPart(m + 1, high - m).assign(hessenBerg.viewColumn(m - 1).viewPart(m + 1, high - m));

                for (int j = m; j <= high; j++) {
                    double g = ort.viewPart(m, high - m + 1).dot(v.viewColumn(j).viewPart(m, high - m + 1));

                    // Double division avoids possible underflow
                    g = g / ort.getX(m) / hessenBerg.getX(m, m - 1);
                    v.viewColumn(j).viewPart(m, high - m + 1).map(ort.viewPart(m, high - m + 1), Functions.plusMult(g));
                }
            }
        }

        return hessenBerg;
    }

    /**
     * Symmetric tridiagonal QL algorithm.
     */
    private void tql2() {
        //  This is derived from the Algol procedures tql2, by
        //  Bowdler, Martin, Reinsch, and Wilkinson, Handbook for
        //  Auto. Comp., Vol.ii-Linear Algebra, and the corresponding
        //  Fortran subroutine in EISPACK.

        e.viewPart(0, n - 1).assign(e.viewPart(1, n - 1));
        e.setX(n - 1, 0.0);

        double f = 0.0;
        double tst1 = 0.0;
        double eps = Math.pow(2.0, -52.0);

        for (int l = 0; l < n; l++) {
            // Find small subdiagonal element.

            tst1 = Math.max(tst1, Math.abs(d.getX(l)) + Math.abs(e.getX(l)));
            int m = l;

            while (m < n) {
                if (Math.abs(e.getX(m)) <= eps * tst1)
                    break;

                m++;
            }

            // If m == l, d.getX(l) is an eigenvalue,
            // otherwise, iterate.

            if (m > l) {
                do {
                    // Compute implicit shift

                    double g = d.getX(l);
                    double p = (d.getX(l + 1) - g) / (2.0 * e.getX(l));
                    double r = Math.hypot(p, 1.0);

                    if (p < 0)
                        r = -r;

                    d.setX(l, e.getX(l) / (p + r));
                    d.setX(l + 1, e.getX(l) * (p + r));
                    double dl1 = d.getX(l + 1);
                    double h = g - d.getX(l);

                    for (int i = l + 2; i < n; i++)
                        d.setX(i, d.getX(i) - h);

                    f += h;

                    // Implicit QL transformation.

                    p = d.getX(m);
                    double c = 1.0;
                    double c2 = c;
                    double c3 = c;
                    double el1 = e.getX(l + 1);
                    double s = 0.0;
                    double s2 = 0.0;

                    for (int i = m - 1; i >= l; i--) {
                        c3 = c2;
                        c2 = c;
                        s2 = s;
                        g = c * e.getX(i);
                        h = c * p;
                        r = Math.hypot(p, e.getX(i));
                        e.setX(i + 1, s * r);
                        s = e.getX(i) / r;
                        c = p / r;
                        p = c * d.getX(i) - s * g;
                        d.setX(i + 1, h + s * (c * g + s * d.getX(i)));

                        // Accumulate transformation.

                        for (int k = 0; k < n; k++) {
                            h = v.getX(k, i + 1);
                            v.setX(k, i + 1, s * v.getX(k, i) + c * h);
                            v.setX(k, i, c * v.getX(k, i) - s * h);
                        }
                    }

                    p = -s * s2 * c3 * el1 * e.getX(l) / dl1;
                    e.setX(l, s * p);
                    d.setX(l, c * p);

                    // Check for convergence.

                }
                while (Math.abs(e.getX(l)) > eps * tst1);
            }

            d.setX(l, d.getX(l) + f);
            e.setX(l, 0.0);
        }

        // Sort eigenvalues and corresponding vectors.

        for (int i = 0; i < n - 1; i++) {
            int k = i;
            double p = d.getX(i);

            for (int j = i + 1; j < n; j++)
                if (d.getX(j) > p) {
                    k = j;
                    p = d.getX(j);
                }

            if (k != i) {
                d.setX(k, d.getX(i));
                d.setX(i, p);

                for (int j = 0; j < n; j++) {
                    p = v.getX(j, i);
                    v.setX(j, i, v.getX(j, k));
                    v.setX(j, k, p);
                }
            }
        }
    }

    /** */
    private void hqr2(Matrix h) {
        //  This is derived from the Algol procedure hqr2,
        //  by Martin and Wilkinson, Handbook for Auto. Comp.,
        //  Vol.ii-Linear Algebra, and the corresponding
        //  Fortran subroutine in EISPACK.

        // Initialize

        int nn = this.n;
        int n = nn - 1;
        int low = 0;
        int high = nn - 1;
        double eps = Math.pow(2.0, -52.0);
        double exshift = 0.0;
        double p = 0;
        double q = 0;
        double r = 0;
        double s = 0;
        double z = 0;
        double w;
        double x;
        double y;

        // Store roots isolated by balanc and compute matrix norm

        double norm = h.foldMap(Functions.PLUS, Functions.ABS, 0.0);

        // Outer loop over eigenvalue index

        int iter = 0;
        while (n >= low) {
            // Look for single small sub-diagonal element
            int l = n;

            while (l > low) {
                s = Math.abs(h.getX(l - 1, l - 1)) + Math.abs(h.getX(l, l));

                if (s == 0.0)
                    s = norm;

                if (Math.abs(h.getX(l, l - 1)) < eps * s)
                    break;

                l--;
            }

            // Check for convergence

            if (l == n) {
                // One root found
                h.setX(n, n, h.getX(n, n) + exshift);
                d.setX(n, h.getX(n, n));
                e.setX(n, 0.0);
                n--;
                iter = 0;
            }
            else if (l == n - 1) {
                // Two roots found
                w = h.getX(n, n - 1) * h.getX(n - 1, n);
                p = (h.getX(n - 1, n - 1) - h.getX(n, n)) / 2.0;
                q = p * p + w;
                z = Math.sqrt(Math.abs(q));
                h.setX(n, n, h.getX(n, n) + exshift);
                h.setX(n - 1, n - 1, h.getX(n - 1, n - 1) + exshift);
                x = h.getX(n, n);

                // Real pair
                if (q >= 0) {
                    if (p >= 0)
                        z = p + z;
                    else
                        z = p - z;

                    d.setX(n - 1, x + z);
                    d.setX(n, d.getX(n - 1));

                    if (z != 0.0)
                        d.setX(n, x - w / z);

                    e.setX(n - 1, 0.0);
                    e.setX(n, 0.0);
                    x = h.getX(n, n - 1);
                    s = Math.abs(x) + Math.abs(z);
                    p = x / s;
                    q = z / s;
                    r = Math.sqrt(p * p + q * q);
                    p /= r;
                    q /= r;

                    // Row modification

                    for (int j = n - 1; j < nn; j++) {
                        z = h.getX(n - 1, j);
                        h.setX(n - 1, j, q * z + p * h.getX(n, j));
                        h.setX(n, j, q * h.getX(n, j) - p * z);
                    }

                    // Column modification

                    for (int i = 0; i <= n; i++) {
                        z = h.getX(i, n - 1);
                        h.setX(i, n - 1, q * z + p * h.getX(i, n));
                        h.setX(i, n, q * h.getX(i, n) - p * z);
                    }

                    // Accumulate transformations

                    for (int i = low; i <= high; i++) {
                        z = v.getX(i, n - 1);
                        v.setX(i, n - 1, q * z + p * v.getX(i, n));
                        v.setX(i, n, q * v.getX(i, n) - p * z);
                    }

                    // Complex pair

                }
                else {
                    d.setX(n - 1, x + p);
                    d.setX(n, x + p);
                    e.setX(n - 1, z);
                    e.setX(n, -z);
                }

                n -= 2;
                iter = 0;

                // No convergence yet

            }
            else {
                // Form shift
                x = h.getX(n, n);
                y = 0.0;
                w = 0.0;

                if (l < n) {
                    y = h.getX(n - 1, n - 1);
                    w = h.getX(n, n - 1) * h.getX(n - 1, n);
                }

                // Wilkinson's original ad hoc shift

                if (iter == 10) {
                    exshift += x;

                    for (int i = low; i <= n; i++)
                        h.setX(i, i, x);

                    s = Math.abs(h.getX(n, n - 1)) + Math.abs(h.getX(n - 1, n - 2));
                    x = y = 0.75 * s;
                    w = -0.4375 * s * s;
                }

                // MATLAB's new ad hoc shift

                if (iter == 30) {
                    s = (y - x) / 2.0;
                    s = s * s + w;

                    if (s > 0) {
                        s = Math.sqrt(s);

                        if (y < x)
                            s = -s;

                        s = x - w / ((y - x) / 2.0 + s);

                        for (int i = low; i <= n; i++)
                            h.setX(i, i, h.getX(i, i) - s);

                        exshift += s;
                        x = y = w = 0.964;
                    }
                }

                iter++;   // (Could check iteration count here.)

                // Look for two consecutive small sub-diagonal elements

                int m = n - 2;

                while (m >= l) {
                    z = h.getX(m, m);
                    r = x - z;
                    s = y - z;
                    p = (r * s - w) / h.getX(m + 1, m) + h.getX(m, m + 1);
                    q = h.getX(m + 1, m + 1) - z - r - s;
                    r = h.getX(m + 2, m + 1);
                    s = Math.abs(p) + Math.abs(q) + Math.abs(r);
                    p /= s;
                    q /= s;
                    r /= s;

                    if (m == l)
                        break;

                    double hmag = Math.abs(h.getX(m - 1, m - 1)) + Math.abs(h.getX(m + 1, m + 1));
                    double threshold = eps * Math.abs(p) * (Math.abs(z) + hmag);

                    if (Math.abs(h.getX(m, m - 1)) * (Math.abs(q) + Math.abs(r)) < threshold)
                        break;

                    m--;
                }

                for (int i = m + 2; i <= n; i++) {
                    h.setX(i, i - 2, 0.0);

                    if (i > m + 2)
                        h.setX(i, i - 3, 0.0);
                }

                // Double QR step involving rows l:n and columns m:n

                for (int k = m; k <= n - 1; k++) {
                    boolean notlast = k != n - 1;

                    if (k != m) {
                        p = h.getX(k, k - 1);
                        q = h.getX(k + 1, k - 1);
                        r = notlast ? h.getX(k + 2, k - 1) : 0.0;
                        x = Math.abs(p) + Math.abs(q) + Math.abs(r);
                        if (x != 0.0) {
                            p /= x;
                            q /= x;
                            r /= x;
                        }
                    }

                    if (x == 0.0)
                        break;

                    s = Math.sqrt(p * p + q * q + r * r);

                    if (p < 0)
                        s = -s;

                    if (s != 0) {
                        if (k != m)
                            h.setX(k, k - 1, -s * x);
                        else if (l != m)
                            h.setX(k, k - 1, -h.getX(k, k - 1));

                        p += s;
                        x = p / s;
                        y = q / s;
                        z = r / s;
                        q /= p;
                        r /= p;

                        // Row modification

                        for (int j = k; j < nn; j++) {
                            p = h.getX(k, j) + q * h.getX(k + 1, j);

                            if (notlast) {
                                p += r * h.getX(k + 2, j);
                                h.setX(k + 2, j, h.getX(k + 2, j) - p * z);
                            }

                            h.setX(k, j, h.getX(k, j) - p * x);
                            h.setX(k + 1, j, h.getX(k + 1, j) - p * y);
                        }

                        // Column modification

                        for (int i = 0; i <= Math.min(n, k + 3); i++) {
                            p = x * h.getX(i, k) + y * h.getX(i, k + 1);

                            if (notlast) {
                                p += z * h.getX(i, k + 2);
                                h.setX(i, k + 2, h.getX(i, k + 2) - p * r);
                            }

                            h.setX(i, k, h.getX(i, k) - p);
                            h.setX(i, k + 1, h.getX(i, k + 1) - p * q);
                        }

                        // Accumulate transformations

                        for (int i = low; i <= high; i++) {
                            p = x * v.getX(i, k) + y * v.getX(i, k + 1);

                            if (notlast) {
                                p += z * v.getX(i, k + 2);
                                v.setX(i, k + 2, v.getX(i, k + 2) - p * r);
                            }

                            v.setX(i, k, v.getX(i, k) - p);
                            v.setX(i, k + 1, v.getX(i, k + 1) - p * q);
                        }
                    }  // (s != 0)
                }  // k loop
            }  // check convergence
        }  // while (n >= low)

        // Back substitute to find vectors of upper triangular form

        if (norm == 0.0)
            return;

        for (n = nn - 1; n >= 0; n--) {
            p = d.getX(n);
            q = e.getX(n);

            // Real vector

            double t;

            if (q == 0) {
                int l = n;
                h.setX(n, n, 1.0);

                for (int i = n - 1; i >= 0; i--) {
                    w = h.getX(i, i) - p;
                    r = 0.0;

                    for (int j = l; j <= n; j++)
                        r += h.getX(i, j) * h.getX(j, n);

                    if (e.getX(i) < 0.0) {
                        z = w;
                        s = r;
                    }
                    else {
                        l = i;

                        if (e.getX(i) == 0.0) {
                            if (w == 0.0)
                                h.setX(i, n, -r / (eps * norm));
                            else
                                h.setX(i, n, -r / w);

                            // Solve real equations

                        }
                        else {
                            x = h.getX(i, i + 1);
                            y = h.getX(i + 1, i);
                            q = (d.getX(i) - p) * (d.getX(i) - p) + e.getX(i) * e.getX(i);
                            t = (x * s - z * r) / q;
                            h.setX(i, n, t);

                            if (Math.abs(x) > Math.abs(z))
                                h.setX(i + 1, n, (-r - w * t) / x);
                            else
                                h.setX(i + 1, n, (-s - y * t) / z);
                        }

                        // Overflow control

                        t = Math.abs(h.getX(i, n));

                        if (eps * t * t > 1) {
                            for (int j = i; j <= n; j++)
                                h.setX(j, n, h.getX(j, n) / t);
                        }
                    }
                }

                // Complex vector

            }
            else if (q < 0) {
                int l = n - 1;

                // Last vector component imaginary so matrix is triangular

                if (Math.abs(h.getX(n, n - 1)) > Math.abs(h.getX(n - 1, n))) {
                    h.setX(n - 1, n - 1, q / h.getX(n, n - 1));
                    h.setX(n - 1, n, -(h.getX(n, n) - p) / h.getX(n, n - 1));
                }
                else {
                    cdiv(0.0, -h.getX(n - 1, n), h.getX(n - 1, n - 1) - p, q);
                    h.setX(n - 1, n - 1, cdivr);
                    h.setX(n - 1, n, cdivi);
                }

                h.setX(n, n - 1, 0.0);
                h.setX(n, n, 1.0);

                for (int i = n - 2; i >= 0; i--) {
                    double ra = 0.0;
                    double sa = 0.0;

                    for (int j = l; j <= n; j++) {
                        ra += h.getX(i, j) * h.getX(j, n - 1);
                        sa += h.getX(i, j) * h.getX(j, n);
                    }

                    w = h.getX(i, i) - p;

                    if (e.getX(i) < 0.0) {
                        z = w;
                        r = ra;
                        s = sa;
                    }
                    else {
                        l = i;

                        if (e.getX(i) == 0) {
                            cdiv(-ra, -sa, w, q);
                            h.setX(i, n - 1, cdivr);
                            h.setX(i, n, cdivi);
                        }
                        else {

                            // Solve complex equations

                            x = h.getX(i, i + 1);
                            y = h.getX(i + 1, i);

                            double vr = (d.getX(i) - p) * (d.getX(i) - p) + e.getX(i) * e.getX(i) - q * q;
                            double vi = (d.getX(i) - p) * 2.0 * q;

                            if (vr == 0.0 && vi == 0.0) {
                                double hmag = Math.abs(x) + Math.abs(y);
                                vr = eps * norm * (Math.abs(w) + Math.abs(q) + hmag + Math.abs(z));
                            }

                            cdiv(x * r - z * ra + q * sa, x * s - z * sa - q * ra, vr, vi);

                            h.setX(i, n - 1, cdivr);
                            h.setX(i, n, cdivi);

                            if (Math.abs(x) > (Math.abs(z) + Math.abs(q))) {
                                h.setX(i + 1, n - 1, (-ra - w * h.getX(i, n - 1) + q * h.getX(i, n)) / x);
                                h.setX(i + 1, n, (-sa - w * h.getX(i, n) - q * h.getX(i, n - 1)) / x);
                            }
                            else {
                                cdiv(-r - y * h.getX(i, n - 1), -s - y * h.getX(i, n), z, q);

                                h.setX(i + 1, n - 1, cdivr);
                                h.setX(i + 1, n, cdivi);
                            }
                        }

                        // Overflow control

                        t = Math.max(Math.abs(h.getX(i, n - 1)), Math.abs(h.getX(i, n)));

                        if (eps * t * t > 1)
                            for (int j = i; j <= n; j++) {
                                h.setX(j, n - 1, h.getX(j, n - 1) / t);
                                h.setX(j, n, h.getX(j, n) / t);
                            }
                    }
                }
            }
        }

        // Vectors of isolated roots

        for (int i = 0; i < nn; i++)
            if (i < low || i > high) {
                for (int j = i; j < nn; j++)
                    v.setX(i, j, h.getX(i, j));
            }

        // Back transformation to get eigen vectors of original matrix

        for (int j = nn - 1; j >= low; j--)
            for (int i = low; i <= high; i++) {
                z = 0.0;

                for (int k = low; k <= Math.min(j, high); k++)
                    z += v.getX(i, k) * h.getX(k, j);

                v.setX(i, j, z);
            }
    }

    /** */
    private static boolean isSymmetric(Matrix matrix) {
        int cols = matrix.columnSize();
        int rows = matrix.rowSize();

        if (cols != rows)
            return false;

        for (int i = 0; i < cols; i++)
            for (int j = 0; j < rows; j++) {
                if (matrix.getX(i, j) != matrix.get(j, i))
                    return false;
            }

        return true;
    }

    /** Complex scalar division - real part. */
    private double cdivr;
    /** Complex scalar division - imaginary part. */
    private double cdivi;

    /** */
    private void cdiv(double xr, double xi, double yr, double yi) {
        double r;
        double d;

        if (Math.abs(yr) > Math.abs(yi)) {
            r = yi / yr;
            d = yr + r * yi;
            cdivr = (xr + r * xi) / d;
            cdivi = (xi - r * xr) / d;
        }
        else {
            r = yr / yi;
            d = yi + r * yr;
            cdivr = (r * xr + xi) / d;
            cdivi = (r * xi - xr) / d;
        }
    }
}

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

/*
Copyright 1999 CERN - European Organization for Nuclear Research.
Permission to use, copy, modify, distribute and sell this software and its documentation for any purpose
is hereby granted without fee, provided that the above copyright notice appear in all copies and
that both that copyright notice and this permission notice appear in supporting documentation.
CERN makes no representations about the suitability of this software for any purpose.
It is provided "as is" without expressed or implied warranty.
*/

package org.apache.ignite.ml.math;

/**
 * Miscellaneous arithmetic and algebra functions.
 * Lifted from Apache Mahout.
 */
public class Algebra extends Constants {
    /** */
    private static final double[] STIRLING_CORRECTION = {
        0.0,
        8.106146679532726e-02, 4.134069595540929e-02,
        2.767792568499834e-02, 2.079067210376509e-02,
        1.664469118982119e-02, 1.387612882307075e-02,
        1.189670994589177e-02, 1.041126526197209e-02,
        9.255462182712733e-03, 8.330563433362871e-03,
        7.573675487951841e-03, 6.942840107209530e-03,
        6.408994188004207e-03, 5.951370112758848e-03,
        5.554733551962801e-03, 5.207655919609640e-03,
        4.901395948434738e-03, 4.629153749334029e-03,
        4.385560249232324e-03, 4.166319691996922e-03,
        3.967954218640860e-03, 3.787618068444430e-03,
        3.622960224683090e-03, 3.472021382978770e-03,
        3.333155636728090e-03, 3.204970228055040e-03,
        3.086278682608780e-03, 2.976063983550410e-03,
        2.873449362352470e-03, 2.777674929752690e-03,
    };

    /** */
    private static final double[] LOG_FACTORIALS = {
        0.00000000000000000, 0.00000000000000000, 0.69314718055994531,
        1.79175946922805500, 3.17805383034794562, 4.78749174278204599,
        6.57925121201010100, 8.52516136106541430, 10.60460290274525023,
        12.80182748008146961, 15.10441257307551530, 17.50230784587388584,
        19.98721449566188615, 22.55216385312342289, 25.19122118273868150,
        27.89927138384089157, 30.67186010608067280, 33.50507345013688888,
        36.39544520803305358, 39.33988418719949404, 42.33561646075348503,
        45.38013889847690803, 48.47118135183522388, 51.60667556776437357,
        54.78472939811231919, 58.00360522298051994, 61.26170176100200198,
        64.55753862700633106, 67.88974313718153498, 71.25703896716800901
    };

    /** */
    private static final long[] LONG_FACTORIALS = {
        1L,
        1L,
        2L,
        6L,
        24L,
        120L,
        720L,
        5040L,
        40320L,
        362880L,
        3628800L,
        39916800L,
        479001600L,
        6227020800L,
        87178291200L,
        1307674368000L,
        20922789888000L,
        355687428096000L,
        6402373705728000L,
        121645100408832000L,
        2432902008176640000L
    };

    /** */
    private static final double[] DOUBLE_FACTORIALS = {
        5.109094217170944E19,
        1.1240007277776077E21,
        2.585201673888498E22,
        6.204484017332394E23,
        1.5511210043330984E25,
        4.032914611266057E26,
        1.0888869450418352E28,
        3.048883446117138E29,
        8.841761993739701E30,
        2.652528598121911E32,
        8.222838654177924E33,
        2.6313083693369355E35,
        8.68331761881189E36,
        2.952327990396041E38,
        1.0333147966386144E40,
        3.719933267899013E41,
        1.3763753091226346E43,
        5.23022617466601E44,
        2.0397882081197447E46,
        8.15915283247898E47,
        3.34525266131638E49,
        1.4050061177528801E51,
        6.041526306337384E52,
        2.6582715747884495E54,
        1.196222208654802E56,
        5.502622159812089E57,
        2.5862324151116827E59,
        1.2413915592536068E61,
        6.082818640342679E62,
        3.0414093201713376E64,
        1.5511187532873816E66,
        8.06581751709439E67,
        4.274883284060024E69,
        2.308436973392413E71,
        1.2696403353658264E73,
        7.109985878048632E74,
        4.052691950487723E76,
        2.350561331282879E78,
        1.386831185456898E80,
        8.32098711274139E81,
        5.075802138772246E83,
        3.146997326038794E85,
        1.9826083154044396E87,
        1.2688693218588414E89,
        8.247650592082472E90,
        5.443449390774432E92,
        3.6471110918188705E94,
        2.48003554243683E96,
        1.7112245242814127E98,
        1.1978571669969892E100,
        8.504785885678624E101,
        6.123445837688612E103,
        4.470115461512686E105,
        3.307885441519387E107,
        2.4809140811395404E109,
        1.8854947016660506E111,
        1.451830920282859E113,
        1.1324281178206295E115,
        8.94618213078298E116,
        7.15694570462638E118,
        5.797126020747369E120,
        4.7536433370128435E122,
        3.94552396972066E124,
        3.314240134565354E126,
        2.8171041143805494E128,
        2.4227095383672744E130,
        2.107757298379527E132,
        1.854826422573984E134,
        1.6507955160908465E136,
        1.4857159644817605E138,
        1.3520015276784033E140,
        1.2438414054641305E142,
        1.156772507081641E144,
        1.0873661566567426E146,
        1.0329978488239061E148,
        9.916779348709491E149,
        9.619275968248216E151,
        9.426890448883248E153,
        9.332621544394415E155,
        9.332621544394418E157,
        9.42594775983836E159,
        9.614466715035125E161,
        9.902900716486178E163,
        1.0299016745145631E166,
        1.0813967582402912E168,
        1.1462805637347086E170,
        1.2265202031961373E172,
        1.324641819451829E174,
        1.4438595832024942E176,
        1.5882455415227423E178,
        1.7629525510902457E180,
        1.974506857221075E182,
        2.2311927486598138E184,
        2.543559733472186E186,
        2.925093693493014E188,
        3.393108684451899E190,
        3.96993716080872E192,
        4.6845258497542896E194,
        5.574585761207606E196,
        6.689502913449135E198,
        8.094298525273444E200,
        9.875044200833601E202,
        1.2146304367025332E205,
        1.506141741511141E207,
        1.882677176888926E209,
        2.3721732428800483E211,
        3.0126600184576624E213,
        3.856204823625808E215,
        4.974504222477287E217,
        6.466855489220473E219,
        8.471580690878813E221,
        1.1182486511960037E224,
        1.4872707060906847E226,
        1.99294274616152E228,
        2.690472707318049E230,
        3.6590428819525483E232,
        5.0128887482749884E234,
        6.917786472619482E236,
        9.615723196941089E238,
        1.3462012475717523E241,
        1.8981437590761713E243,
        2.6953641378881633E245,
        3.8543707171800694E247,
        5.550293832739308E249,
        8.047926057471989E251,
        1.1749972043909107E254,
        1.72724589045464E256,
        2.5563239178728637E258,
        3.8089226376305687E260,
        5.7133839564458575E262,
        8.627209774233244E264,
        1.3113358856834527E267,
        2.0063439050956838E269,
        3.0897696138473515E271,
        4.789142901463393E273,
        7.471062926282892E275,
        1.1729568794264134E278,
        1.8532718694937346E280,
        2.946702272495036E282,
        4.714723635992061E284,
        7.590705053947223E286,
        1.2296942187394494E289,
        2.0044015765453032E291,
        3.287218585534299E293,
        5.423910666131583E295,
        9.003691705778434E297,
        1.5036165148649983E300,
        2.5260757449731988E302,
        4.2690680090047056E304,
        7.257415615308004E306
    };

    /**
     * Efficiently returns the binomial coefficient, often also referred to as
     * "n over k" or "n choose k". The binomial coefficient is defined as
     * {@code (n * n-1 * ... * n-k+1 ) / ( 1 * 2 * ... * k )}.
     * <ul> <li>{@code k&lt;0}: {@code 0}.</li>
     * <li>{@code k==0}: {@code 1}.</li>
     * <li>{@code k==1}: {@code n}.</li>
     * <li>else: {@code (n * n-1 * ... * n-k+1 ) / ( 1 * 2 * ... * k)}.</li>
     * </ul>
     *
     * @param n Size of set.
     * @param k Size of subset.
     * @return Binomial coefficient.
     */
    public static double binomial(double n, long k) {
        if (k < 0)
            return 0;

        if (k == 0)
            return 1;

        if (k == 1)
            return n;

        // binomial(n,k) = (n * n-1 * ... * n-k+1 ) / ( 1 * 2 * ... * k )
        double a = n - k + 1;
        double b = 1;
        double binomial = 1;

        for (long i = k; i-- > 0; )
            binomial *= (a++) / (b++);

        return binomial;
    }

    /**
     * Efficiently returns the binomial coefficient, often also referred to as "n over k" or "n choose k".
     * The binomial coefficient is defined as
     * <ul> <li>{@code k&lt;0}: {@code 0}. <li>{@code k==0 || k==n}: {@code 1}. <li>{@code k==1 || k==n-1}:
     * {@code n}. <li>else: {@code (n * n-1 * ... * n-k+1 ) / ( 1 * 2 * ... * k )}. </ul>
     *
     * @param n Size of set.
     * @param k Size of subset.
     * @return Binomial coefficient.
     */
    public static double binomial(long n, long k) {
        if (k < 0)
            return 0;

        if (k == 0 || k == n)
            return 1;

        if (k == 1 || k == n - 1)
            return n;

        if (n > k) {
            int max = LONG_FACTORIALS.length + DOUBLE_FACTORIALS.length;

            if (n < max) {
                double nFac = factorial((int)n);
                double kFac = factorial((int)k);
                double nMinusKFac = factorial((int)(n - k));
                double nk = nMinusKFac * kFac;

                if (nk != Double.POSITIVE_INFINITY) // No numeric overflow?
                    return nFac / nk;
            }

            if (k > n / 2)
                k = n - k;
        }

        // binomial(n,k) = (n * n-1 * ... * n-k+1 ) / ( 1 * 2 * ... * k )
        long a = n - k + 1;
        long b = 1;
        double binomial = 1;

        for (long i = k; i-- > 0; )
            binomial *= (double)a++ / (b++);

        return binomial;
    }

    /**
     * Returns the smallest <code>long &gt;= value</code>.
     * <dl><dt>Examples: {@code 1.0 -> 1, 1.2 -> 2, 1.9 -> 2}. This
     * method is safer than using (long) Math.ceil(value), because of possible rounding error.</dt></dl>
     *
     * @param val Value for ceil.
     * @return Ceil of the given value.
     */
    public static long ceil(double val) {
        return Math.round(Math.ceil(val));
    }

    /**
     * Evaluates the series of Chebyshev polynomials Ti at argument x/2. The series is given by
     * <pre class="snippet">
     * N-1
     * - '
     * y  =   &gt;   coef[i] T (x/2)
     * -            i
     * i=0
     * </pre>
     * Coefficients are stored in reverse order, i.e. the zero order term is last in the array.  Note N is the number of
     * coefficients, not the order.
     * <p>
     * If coefficients are for the interval a to b, x must have been transformed to x
     * -&lt; 2(2x - b - a)/(b-a) before entering the routine.  This maps x from (a, b) to (-1, 1), over which the
     * Chebyshev polynomials are defined.</p>
     * <p>
     * If the coefficients are for the inverted interval, in which (a, b) is
     * mapped to (1/b, 1/a), the transformation required is {@code x -> 2(2ab/x - b - a)/(b-a)}.  If b is infinity, this
     * becomes {@code x -> 4a/x - 1}.</p>
     * <p>
     * SPEED:
     * </p>
     * Taking advantage of the recurrence properties of the Chebyshev
     * polynomials, the routine requires one more addition per loop than evaluating a nested polynomial of the same
     * degree.
     *
     * @param x Argument to the polynomial.
     * @param coef Coefficients of the polynomial.
     * @param N Number of coefficients.
     */
    public static double chbevl(double x, double[] coef, int N) {
        int p = 0;

        double b0 = coef[p++];
        double b1 = 0.0;
        int i = N - 1;

        double b2;

        do {
            b2 = b1;
            b1 = b0;
            b0 = x * b1 - b2 + coef[p++];
        }
        while (--i > 0);

        return 0.5 * (b0 - b2);
    }

    /**
     * Instantly returns the factorial {@code k!}.
     *
     * @param k must hold {@code k &gt;= 0}.
     */
    private static double factorial(int k) {
        if (k < 0)
            throw new IllegalArgumentException();

        int len1 = LONG_FACTORIALS.length;

        if (k < len1)
            return LONG_FACTORIALS[k];

        int len2 = DOUBLE_FACTORIALS.length;

        return (k < len1 + len2) ? DOUBLE_FACTORIALS[k - len1] : Double.POSITIVE_INFINITY;
    }

    /**
     * Returns the largest <code>long &lt;= value</code>.
     * <dl><dt>Examples: {@code 1.0 -> 1, 1.2 -> 1, 1.9 -> 1 <dt> 2.0 -> 2, 2.2 -> 2, 2.9 -> 2}</dt></dl>
     * This method is safer than using (long) Math.floor(value), because of possible rounding error.
     */
    public static long floor(double val) {
        return Math.round(Math.floor(val));
    }

    /**
     * Returns {@code log<sub>base</sub>value}.
     */
    public static double log(double base, double val) {
        return Math.log(val) / Math.log(base);
    }

    /**
     * Returns {@code log<sub>10</sub>value}.
     */
    public static double log10(double val) {
        // 1.0 / Math.log(10) == 0.43429448190325176
        return Math.log(val) * 0.43429448190325176;
    }

    /**
     * Returns {@code log<sub>2</sub>value}.
     */
    public static double log2(double val) {
        // 1.0 / Math.log(2) == 1.4426950408889634
        return Math.log(val) * 1.4426950408889634;
    }

    /**
     * Returns {@code log(k!)}. Tries to avoid overflows. For {@code k&lt;30} simply looks up a table in O(1).
     * For {@code k&gt;=30} uses Stirling's approximation.
     *
     * @param k must hold {@code k &gt;= 0}.
     */
    public static double logFactorial(int k) {
        if (k >= 30) {
            double r = 1.0 / k;
            double rr = r * r;
            double C7 = -5.95238095238095238e-04;
            double C5 = 7.93650793650793651e-04;
            double C3 = -2.77777777777777778e-03;
            double C1 = 8.33333333333333333e-02;
            double C0 = 9.18938533204672742e-01;

            return (k + 0.5) * Math.log(k) - k + C0 + r * (C1 + rr * (C3 + rr * (C5 + rr * C7)));
        }
        else
            return LOG_FACTORIALS[k];
    }

    /**
     * Instantly returns the factorial {@code k!}.
     *
     * @param k must hold {@code k >= 0 && k < 21}
     */
    public static long longFactorial(int k) {
        if (k < 0)
            throw new IllegalArgumentException("Negative k");

        if (k < LONG_FACTORIALS.length)
            return LONG_FACTORIALS[k];

        throw new IllegalArgumentException("Overflow");
    }

    /**
     * Returns the StirlingCorrection.
     * <p>
     * Correction term of the Stirling approximation for {@code log(k!)} (series in
     * 1/k, or table values for small k) with int parameter k. {@code  log k! = (k + 1/2)log(k + 1) - (k + 1) +
     * (1/2)log(2Pi) + STIRLING_CORRECTION(k + 1) log k! = (k + 1/2)log(k)     -  k      + (1/2)log(2Pi) +
     * STIRLING_CORRECTION(k) } </p>
     */
    public static double stirlingCorrection(int k) {
        if (k > 30) {
            double r = 1.0 / k;
            double rr = r * r;
            double C7 = -5.95238095238095238e-04;
            double C5 = 7.93650793650793651e-04;
            double C3 = -2.77777777777777778e-03;
            double C1 = 8.33333333333333333e-02;

            return r * (C1 + rr * (C3 + rr * (C5 + rr * C7)));
        }
        else
            return STIRLING_CORRECTION[k];
    }

    /**
     * Evaluates the given polynomial of degree {@code N} at {@code x}, assuming coefficient of N is 1.0. Otherwise same
     * as {@link #evalPoly(double, double[], int)}.
     * <pre class="snippet">
     * 2          N
     * y  =  C  + C x + C x  +...+ C x
     * 0    1     2          N
     * </pre>
     * where <pre class="snippet">
     * C  = 1
     * N
     * </pre>
     * and hence is omitted from the array.
     * <p>
     * Coefficients are stored in reverse order:</p>
     * <pre class="snippet">
     * coef[0] = C  , ..., coef[N-1] = C  .
     * N-1                   0
     * </pre>
     * Calling arguments are otherwise the same as {@link #evalPoly(double, double[], int)}.
     * <p>
     * In the interest of speed, there are no checks for out of bounds arithmetic.
     *
     * @param x Argument to the polynomial.
     * @param coef Coefficients of the polynomial.
     * @param n Degree of the polynomial.
     */
    public static double evalPoly1(double x, double[] coef, int n) {
        double res = x + coef[0];

        for (int i = 1; i < n; i++)
            res = res * x + coef[i];

        return res;
    }

    /**
     * Evaluates the given polynomial of degree {@code N} at {@code x}.
     * <pre class="snippet">
     * 2          N
     * y  =  C  + C x + C x  +...+ C x
     * 0    1     2          N
     * </pre>
     * <p>
     * Coefficients are stored in reverse order:</p>
     * <pre class="snippet">
     * coef[0] = C  , ..., coef[N] = C  .
     * N                   0
     * </pre>
     * <p>
     * In the interest of speed, there are no checks for out of bounds arithmetic.</p>
     *
     * @param x Argument to the polynomial.
     * @param coef Coefficients of the polynomial.
     * @param n Degree of the polynomial.
     */
    public static double evalPoly(double x, double[] coef, int n) {
        double res = coef[0];

        for (int i = 1; i <= n; i++)
            res = res * x + coef[i];

        return res;
    }

    /**
     * Gets <code>sqrt(a^2 + b^2)</code> without under/overflow.
     *
     * @param a First side value.
     * @param b Second side value.
     * @return Hypotenuse value.
     */
    public static double hypot(double a, double b) {
        double r;

        if (Math.abs(a) > Math.abs(b)) {
            r = b / a;
            r = Math.abs(a) * Math.sqrt(1 + r * r);
        }
        else if (b != 0) {
            r = a / b;
            r = Math.abs(b) * Math.sqrt(1 + r * r);
        }
        else
            r = 0.0;

        return r;
    }
}

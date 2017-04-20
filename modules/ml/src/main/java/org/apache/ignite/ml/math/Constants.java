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
 * Math constants. Lifted from Apache Mahout.
 */
public class Constants {
    /** Constant for {@code 2**-53}. */
    public static final double MACHEP = 1.11022302462515654042E-16;

    /** Constant for {@code log(2**1024)}. */
    public static final double MAXLOG = 7.09782712893383996732E2;

    /** Constant for {@code log(2**-1022)}. */
    public static final double MINLOG = -7.451332191019412076235E2;

    /** Constant for gamma function. */
    public static final double MAXGAM = 171.624376956302725;

    /** Constant for {@code 1/(sqrt(2*pi))}. */
    public static final double SQTPI = 2.50662827463100050242E0;

    /** Constant for {@code sqrt(2)/2}. */
    public static final double SQRTH = 7.07106781186547524401E-1;

    /** Constant for {@code log(Pi)}. */
    public static final double LOGPI = 1.14472988584940017414;

    /** Constant for big value. */
    public static final double BIG = 4.503599627370496e15;

    /** Constant for inverse of big value. */
    public static final double BIGINV = 2.22044604925031308085e-16;
}

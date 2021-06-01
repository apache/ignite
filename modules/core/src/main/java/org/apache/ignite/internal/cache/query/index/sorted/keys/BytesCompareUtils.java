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

package org.apache.ignite.internal.cache.query.index.sorted.keys;

/** */
public final class BytesCompareUtils {
    /** */
    public static int compareNotNullSigned(byte[] arr0, byte[] arr1) {
        if (arr0 == arr1)
            return 0;
        else {
            int commonLen = Math.min(arr0.length, arr1.length);

            for (int i = 0; i < commonLen; ++i) {
                byte b0 = arr0[i];
                byte b1 = arr1[i];

                if (b0 != b1)
                    return b0 > b1 ? 1 : -1;
            }

            return Integer.signum(arr0.length - arr1.length);
        }
    }

    /** */
    public static int compareNotNullUnsigned(byte[] arr0, byte[] arr1) {
        if (arr0 == arr1)
            return 0;
        else {
            int commonLen = Math.min(arr0.length, arr1.length);

            for (int i = 0; i < commonLen; ++i) {
                int unSignArr0 = arr0[i] & 255;
                int unSignArr1 = arr1[i] & 255;

                if (unSignArr0 != unSignArr1)
                    return unSignArr0 > unSignArr1 ? 1 : -1;
            }

            return Integer.signum(arr0.length - arr1.length);
        }
    }
}

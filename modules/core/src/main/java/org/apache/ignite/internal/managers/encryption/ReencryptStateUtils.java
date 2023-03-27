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

package org.apache.ignite.internal.managers.encryption;

/** */
public class ReencryptStateUtils {
    /**
     * @param idx Index of the last reencrypted page.
     * @param total Total pages to be reencrypted.
     * @return Reencryption status.
     */
    public static long state(int idx, int total) {
        return ((long)idx) << Integer.SIZE | (total & 0xffffffffL);
    }

    /**
     * @param state Reencryption status.
     * @return Index of the last reencrypted page.
     */
    public static int pageIndex(long state) {
        return (int)(state >> Integer.SIZE);
    }

    /**
     * @param state Reencryption status.
     * @return Total pages to be reencrypted.
     */
    public static int pageCount(long state) {
        return (int)state;
    }
}

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
public class ReencryptState {
    /** Index of the last reencrypted page. */
    private final int idx;

    /** Total pages to be reencrypted. */
    private final int cnt;

    /**
     * @param idx Index of the last reencrypted page.
     * @param cnt Total pages to be reencrypted.
     */
    public ReencryptState(int idx, int cnt) {
        this.idx = idx;
        this.cnt = cnt;
    }

    /**
     * @return Total pages to be reencrypted.
     */
    public int pageCount() {
        return cnt;
    }

    /**
     * @return Index of the last reencrypted page.
     */
    public int pageIndex() {
        return idx;
    }
}

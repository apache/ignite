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

package org.apache.ignite.internal.util.tostring;

import org.apache.ignite.IgniteSystemProperties;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_TO_STRING_MAX_LENGTH;

/**
 *
 */
class SBLengthLimit {
    /** */
    private static final int MAX_TO_STR_LEN = IgniteSystemProperties.getInteger(IGNITE_TO_STRING_MAX_LENGTH, 10_000);

    /** */
    private int len;

    /** */
    private boolean done;

    /**
     * @return Current length.
     */
    int length() {
        return len;
    }

    /**
     *
     */
    void reset() {
        len = 0;
        done = false;
    }

    /**
     * @param sb String builder.
     * @param writtenLen Written length.
     */
    void onWrite(SBLimitedLength sb, int writtenLen) {
        if (done)
            return;

        len += writtenLen;

        if (len > MAX_TO_STR_LEN) {
            sb.appendNoLimitCheck("...");

            done = true;
        }
    }

    /**
     * @return {@code True} if reached limit.
     */
    boolean done() {
        return done;
    }
}

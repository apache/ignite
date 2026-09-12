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

import org.apache.ignite.IgniteCommonsSystemProperties;

import static org.apache.ignite.internal.util.tostring.GridToStringBuilder.DFLT_TO_STRING_MAX_LENGTH;

/**
 *
 */
class SBLengthLimit {
    /** */
    private static final int MAX_TO_STR_LEN =
        IgniteCommonsSystemProperties.getInteger(IgniteCommonsSystemProperties.IGNITE_TO_STRING_MAX_LENGTH, DFLT_TO_STRING_MAX_LENGTH);

    /** Length of tail part of message. */
    private static final int TAIL_LEN = MAX_TO_STR_LEN / 10 * 2;

    /** Length of head part of message. */
    private static final int HEAD_LEN = MAX_TO_STR_LEN - TAIL_LEN;

    /**
     * @param sb String builder.
     * @param writtenLen Written length.
     */
    void onWrite(SBLimitedLength sb, int writtenLen) {
        if (overflowed(sb) && (sb.getTail() == null || sb.getTail().length() == 0)) {
            CircularStringBuilder tail = createTail();
            int newSbLen = Math.min(sb.length(), getHeadLengthLimit());
            tail.append(sb.impl().substring(newSbLen));
            sb.setTail(tail);
            sb.impl().setLength(newSbLen);
        }
    }

    /** Creates empty tail
     * @return empty tail */
    CircularStringBuilder createTail() {
        return new CircularStringBuilder(getTailLengthLimit());
    }

    /**
     * @return {@code True} if this string builder exceeds limit, false otherwise
     */
    boolean overflowed(SBLimitedLength sb) {
        return sb.length() >= getHeadLengthLimit();
    }

    /**
     * Returns max available head length
     * @return head limit */
    int getHeadLengthLimit() {
        return HEAD_LEN;
    }

    /**
     *
     */
    int getTailLengthLimit() {
        return TAIL_LEN;
    }
}

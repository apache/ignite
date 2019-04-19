/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.util.tostring;

/**
 * Helper wrapper containing StringBuilder and additional values. Stored as a thread-local variable.
 */
class GridToStringThreadLocal {
    /** */
    private SBLimitedLength sb = new SBLimitedLength(256);

    /** */
    private Object[] addNames = new Object[7];

    /** */
    private Object[] addVals = new Object[7];

    /** */
    private boolean[] addSens = new boolean[7];

    /**
     * @param len Length limit.
     * @return String builder.
     */
    SBLimitedLength getStringBuilder(SBLengthLimit len) {
        sb.initLimit(len);

        return sb;
    }

    /**
     * @return Additional names.
     */
    Object[] getAdditionalNames() {
        return addNames;
    }

    /**
     * @return Additional values.
     */
    Object[] getAdditionalValues() {
        return addVals;
    }

    /**
     * @return Additional values.
     */
    boolean[] getAdditionalSensitives() {
        return addSens;
    }
}
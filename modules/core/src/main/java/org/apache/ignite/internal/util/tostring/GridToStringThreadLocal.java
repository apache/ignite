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

import org.apache.ignite.internal.util.typedef.internal.SB;

/**
 * Helper wrapper containing StringBuilder and additional values. Stored as a thread-local variable.
 */
class GridToStringThreadLocal {
    /** */
    private SB sb = new SB(256);

    /** */
    private Object[] addNames = new Object[7];

    /** */
    private Object[] addVals = new Object[7];

    /** */
    private boolean[] addSens = new boolean[7];

    /**
     * @return String builder.
     */
    SB getStringBuilder() {
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
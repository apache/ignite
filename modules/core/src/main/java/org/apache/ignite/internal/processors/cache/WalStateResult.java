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

package org.apache.ignite.internal.processors.cache;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Local WAL state change result.
 */
public class WalStateResult {
    /** Original message. */
    private final WalStateProposeMessage msg;

    /** Detailed change WAL state results per group. */
    private final Map<Integer, Boolean> grpResults;

    /** Error message (if any). */
    private final String errMsg;

    /**
     * Constructor for successful operation.
     *
     * @param msg Original message.
     * @param grpResults Detailed change WAL state results per group.
     */
    public WalStateResult(WalStateProposeMessage msg, Map<Integer, Boolean> grpResults) {
        this(msg, grpResults, null);
    }

    /**
     * Constructor for error.
     *
     * @param msg Original message.
     * @param errMsg Error message (if any).
     */
    public WalStateResult(WalStateProposeMessage msg, String errMsg) {
        this(msg, Collections.emptyMap(), errMsg);
    }

    /**
     * Main constructor.
     *
     * @param msg Original message.
     * @param grpResults Detailed results per group.
     * @param errMsg Error message (if any).
     */
    private WalStateResult(WalStateProposeMessage msg, Map<Integer, Boolean> grpResults, String errMsg) {
        this.msg = msg;
        this.grpResults = grpResults != null ? new HashMap<>(grpResults) : Collections.emptyMap();
        this.errMsg = errMsg;
    }

    /**
     * @return Original message.
     */
    public WalStateProposeMessage message() {
        return msg;
    }

    /**
     * @return Detailed results per group.
     */
    public Map<Integer, Boolean> groupResults() {
        return grpResults;
    }

    /**
     * @return Error message (if any).
     */
    @Nullable public String errorMessage() {
        return errMsg;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(WalStateResult.class, this);
    }
}

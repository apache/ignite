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

import org.jetbrains.annotations.Nullable;

/**
 * Local WAL state change result.
 */
public class WalStateResult {
    /** Original message. */
    private final WalStateProposeMessage msg;

    /** Whether mode was changed. */
    private final boolean res;

    /** Error message (if any). */
    private final String errMsg;

    /** Whether this is no-op result which bypasses distributed completion. */
    private final boolean noOp;

    /**
     * Constructor.
     *
     * @param msg Original message.
     * @param res Whether mode was changed.
     * @param noOp Whether this is no-op result which bypasses distributed completion.
     */
    public WalStateResult(WalStateProposeMessage msg, boolean res, boolean noOp) {
        this(msg, res, null, noOp);
    }

    /**
     * Constructor.
     *
     * @param msg Original message.
     * @param errMsg Error message (if any).
     * @param noOp Whether this is no-op result which bypasses distributed completion.
     */
    public WalStateResult(WalStateProposeMessage msg, String errMsg, boolean noOp) {
        this(msg, false, errMsg, noOp);
    }

    /**
     * Constructor.
     *
     * @param msg Original message.
     * @param res Whether mode was changed.
     * @param errMsg Error message (if any).
     * @param noOp Whether this is no-op result which bypasses distributed completion.
     */
    private WalStateResult(WalStateProposeMessage msg, boolean res, String errMsg, boolean noOp) {
        this.msg = msg;
        this.res = res;
        this.errMsg = errMsg;
        this.noOp = noOp;
    }

    /**
     * @return Original message.
     */
    public WalStateProposeMessage message() {
        return msg;
    }

    /**
     * @return Whether mode was changed.
     */
    public boolean result() {
        return res;
    }

    /**
     * @return Error message (if any).
     */
    @Nullable public String errorMessage() {
        return errMsg;
    }

    /**
     * @return Whether this is no-op result which bypasses distributed completion.
     */
    public boolean noOp() {
        return noOp;
    }
}

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

package org.apache.ignite.internal.metastorage.common.command;

import java.util.List;
import org.apache.ignite.raft.client.WriteCommand;

/**
 * Represents invoke command for meta storage.
 */
public class InvokeCommand implements WriteCommand {
    /** Condition. */
    private final ConditionInfo cond;

    /** Success operations. */
    private final List<OperationInfo> success;

    /** Failure operations. */
    private final List<OperationInfo> failure;

    /**
     * Constructs invoke command instance.
     *
     * @param cond    Condition.
     * @param success Success operations.
     * @param failure Failure operations.
     */
    public InvokeCommand(ConditionInfo cond, List<OperationInfo> success, List<OperationInfo> failure) {
        this.cond = cond;
        this.success = success;
        this.failure = failure;
    }

    /**
     * Returns condition.
     *
     * @return Condition.
     */
    public ConditionInfo condition() {
        return cond;
    }

    /**
     * Returns success operations.
     *
     * @return Success operations.
     */
    public List<OperationInfo> success() {
        return success;
    }

    /**
     * Returns failure operations.
     *
     * @return Failure operations.
     */
    public List<OperationInfo> failure() {
        return failure;
    }
}

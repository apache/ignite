/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.raft.client.exception;

import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.raft.client.RaftErrorCode;

/**
 * A raft exception containing code and description.
 */
public class RaftException extends IgniteInternalException {
    private final RaftErrorCode code;

    /**
     * @param errCode Error code.
     */
    public RaftException(RaftErrorCode errCode) {
        this.code = errCode;
    }

    /**
     * @param errCode Error code.
     * @param message Error message.
     */
    public RaftException(RaftErrorCode errCode, String message) {
        super(message);

        this.code = errCode;
    }

    /**
     * @return Error code.
     */
    public RaftErrorCode errorCode() {
        return code;
    }
}

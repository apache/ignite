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
package org.apache.ignite.raft.jraft.rpc;

import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.error.RaftError;

/**
 * TODO asch refactor Helper to create error response https://issues.apache.org/jira/browse/IGNITE-14838
 */
public interface RpcResponseFactory {
    /**
     * This is a convention that if a {@link Message} contains an {@link RpcRequests.ErrorResponse} field, it can only
     * be in position 99.
     */
    int ERROR_RESPONSE_NUM = 99;

    /**
     * Creates a RPC response from status, return OK response when status is null.
     *
     * @param parent parent message
     * @param st status with response
     * @return a response instance
     */
    default Message newResponse(final Message parent, final Status st) {
        if (st == null) {
            return newResponse(parent, 0, "OK");
        }
        return newResponse(parent, st.getCode(), st.getErrorMsg());
    }

    /**
     * Creates an error response with parameters.
     *
     * @param parent parent message
     * @param error error with raft info
     * @param fmt message with format string
     * @param args arguments referenced by the format specifiers in the format string
     * @return a response instance
     */
    default Message newResponse(final Message parent, final RaftError error, final String fmt, final Object... args) {
        return newResponse(parent, error.getNumber(), fmt, args);
    }

    /**
     * Creates an error response with parameters.
     *
     * @param parent parent message
     * @param code error code with raft info
     * @param fmt message with format string
     * @param args arguments referenced by the format specifiers in the format string
     * @return a response instance
     */
    default Message newResponse(final Message parent, final int code, final String fmt, final Object... args) {
        final RpcRequests.ErrorResponse.Builder eBuilder = RpcRequests.ErrorResponse.newBuilder();
        eBuilder.setErrorCode(code);
        if (fmt != null) {
            eBuilder.setErrorMsg(String.format(fmt, args));
        }
        return eBuilder.build();
    }
}

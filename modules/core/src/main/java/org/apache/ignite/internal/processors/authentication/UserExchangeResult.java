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
package org.apache.ignite.internal.processors.authentication;

import org.apache.ignite.IgniteCheckedException;

/**
 *
 */
public class UserExchangeResult {
    /**  */
    private final UserManagementOperation op;

    /** */
    private final ResultType resType;

    /** */
    private final IgniteCheckedException error;

    /** */
    private enum ResultType {
        /** */
        SUCCESS,

        /** */
        FAILURE,

        /** */
        EXCHANGE_DISABLED
    }

    /**
     * @param resType Result type.
     * @param op User management operation.
     * @param error Error.
     */
    private UserExchangeResult(ResultType resType, UserManagementOperation op, IgniteCheckedException error) {
        this.resType = resType;
        this.op = op;
        this.error = error;
    }

    /**
     * @return Exchange user result.
     */
    public UserManagementOperation operation() {
        return op;
    }

    /**
     * @return Exchange error reason.
     */
    public IgniteCheckedException error() {
        return error;
    }

    /**
     * @return Successful exchange flag.
     */
    public boolean successful() {
        return resType == ResultType.SUCCESS;
    }

    /**
     * @return Exchange disabled flag.
     */
    public boolean exchangeDisabled() {
        return resType == ResultType.EXCHANGE_DISABLED;
    }

    /**
     * @param op User management operation.
     * @return Exchange result.
     */
    static UserExchangeResult createSuccessfulResult(UserManagementOperation op) {
        assert op != null;

        return new UserExchangeResult(ResultType.SUCCESS, op, null);
    }

    /**
     * @param error Error.
     * @return Exchange result.
     */
    static UserExchangeResult createFailureResult(IgniteCheckedException error) {
        assert error != null;

        return new UserExchangeResult(ResultType.FAILURE, null, error);
    }

    /**
     * @return Exchange result.
     */
    static UserExchangeResult createExchangeDisabledResult() {
        return new UserExchangeResult(ResultType.EXCHANGE_DISABLED, null, null);
    }
}

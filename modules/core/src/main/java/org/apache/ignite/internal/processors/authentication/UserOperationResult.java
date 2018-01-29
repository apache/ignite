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
import org.apache.ignite.lang.IgniteUuid;

/**
 *
 */
public class UserOperationResult {
    /**  */
    private final IgniteUuid opId;

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
     * @param opId User management operation ID.
     * @param error Error.
     */
    private UserOperationResult(ResultType resType, IgniteUuid opId, IgniteCheckedException error) {
        this.resType = resType;
        this.opId = opId;
        this.error = error;
    }

    /**
     * @return Operation ID.
     */
    public IgniteUuid operationId() {
        return opId;
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
     * @param opId User management operation ID.
     * @return Exchange result.
     */
    static UserOperationResult createSuccessfulResult(IgniteUuid opId) {
        assert opId != null;

        return new UserOperationResult(ResultType.SUCCESS, opId, null);
    }

    /**
     * @param error Error.
     * @return Exchange result.
     */
    static UserOperationResult createFailureResult(IgniteCheckedException error) {
        assert error != null;

        return new UserOperationResult(ResultType.FAILURE, null, error);
    }

    /**
     * @return Exchange result.
     */
    static UserOperationResult createExchangeDisabledResult() {
        return new UserOperationResult(ResultType.EXCHANGE_DISABLED, null, null);
    }
}

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
package org.apache.ignite.internal.processors.cache.binary;

import org.apache.ignite.binary.BinaryObjectException;

/**
 * Represents result of metadata update or metadata read request (so it is used both by server and client nodes).
 */
final class MetadataUpdateResult {
    /** */
    private final ResultType resType;

    /** */
    private final BinaryObjectException error;

    /** */
    private final int typeVer;

    /**
     * @param resType Response type.
     * @param error Error.
     * @param typeVer Accepted version of updated type.
     */
    private MetadataUpdateResult(ResultType resType, BinaryObjectException error, int typeVer) {
        this.resType = resType;
        this.error = error;
        this.typeVer = typeVer;
    }

    /**
     *
     */
    boolean rejected() {
        return resType == ResultType.REJECT;
    }

    /**
     *
     */
    BinaryObjectException error() {
        return error;
    }

    /** */
    int typeVersion() {
        return typeVer;
    }

    /**
     * @param typeVer Accepted version of updated BinaryMetadata type or <code>-1</code> if not applicable.
     */
    static MetadataUpdateResult createSuccessfulResult(int typeVer) {
        return new MetadataUpdateResult(ResultType.SUCCESS, null, typeVer);
    }

    /**
     * @param err Error lead to request failure.
     */
    static MetadataUpdateResult createFailureResult(BinaryObjectException err) {
        assert err != null;

        return new MetadataUpdateResult(ResultType.REJECT, err, -1);
    }

    /**
     *
     */
    static MetadataUpdateResult createUpdateDisabledResult() {
        return new MetadataUpdateResult(ResultType.UPDATE_DISABLED, null, -1);
    }

    /**
     *
     */
    private enum ResultType {
        /**
         * If request completed successfully.
         */
        SUCCESS,

        /**
         * If request was rejected for any reason.
         */
        REJECT,

        /**
         * If request arrived at the moment when node has been stopping.
         */
        UPDATE_DISABLED
    }
}

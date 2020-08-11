/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.checkpoint;

/**
 * Represent a reason by that a WAL history was bounded.
 */
public enum ReservationReason {
    /** The message puts down to log when an exception happened during
     * reading a WAL segment or a segment cannot be reserved. */
    WAL_RESERVATION_ERROR,

    /** Reason means no more history reserved for the cache. */
    NO_MORE_HISTORY,

    /** Reason means a checkpoint in history reserved can not be applied for cache. */
    CHECKPOINT_NOT_APPLICABLE;

    /** {@inheritDoc} */
    @Override public String toString() {
        switch (this) {
            case WAL_RESERVATION_ERROR:
                return "Unexpected error during processing of previous checkpoint";

            case NO_MORE_HISTORY:
                return "Reserved checkpoint is the oldest in history";

            case CHECKPOINT_NOT_APPLICABLE:
                return "Checkpoint was marked as inapplicable for historical rebalancing";

            default:
                throw new IllegalArgumentException();
        }
    }
}

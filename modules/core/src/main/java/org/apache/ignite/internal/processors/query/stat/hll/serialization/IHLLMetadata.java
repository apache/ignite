/*
 * Copyright 2013 Aggregate Knowledge, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.query.stat.hll.serialization;

import org.apache.ignite.internal.processors.query.stat.hll.HLLType;

/**
 * The metadata and parameters associated with a HLL.
 */
public interface IHLLMetadata {
    /**
     * @return the schema version of the HLL. This will never be <code>null</code>.
     */
    int schemaVersion();

    /**
     * @return the type of the HLL. This will never be <code>null</code>.
     */
    HLLType HLLType();

    /**
     * @return the log-base-2 of the register count parameter of the HLL. This
     *         will always be greater than or equal to 4 and less than or equal
     *         to 31.
     */
    int registerCountLog2();

    /**
     * @return the register width parameter of the HLL. This will always be
     *         greater than or equal to 1 and less than or equal to 8.
     */
    int registerWidth();

    /**
     * @return the log-base-2 of the explicit cutoff cardinality. This will always
     *         be greater than or equal to zero and less than 31, per the specification.
     */
    int log2ExplicitCutoff();

    /**
     * @return <code>true</code> if the {@link HLLType#EXPLICIT} representation
     *         has been disabled. <code>false</code> otherwise.
     */
    boolean explicitOff();

    /**
     * @return <code>true</code> if the {@link HLLType#EXPLICIT} representation
     *         cutoff cardinality is set to be automatically chosen,
     *         <code>false</code> otherwise.
     */
    boolean explicitAuto();

    /**
     * @return <code>true</code> if the {@link HLLType#SPARSE} representation
     *         is enabled.
     */
    boolean sparseEnabled();
}

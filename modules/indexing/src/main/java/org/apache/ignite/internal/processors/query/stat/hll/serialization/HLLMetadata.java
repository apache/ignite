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
 * A concrete {@link IHLLMetadata} implemented as a simple struct.
 *
 * @author timon
 */
public class HLLMetadata implements IHLLMetadata {
    private final int schemaVersion;
    private final HLLType type;
    private final int registerCountLog2;
    private final int registerWidth;
    private final int log2ExplicitCutoff;
    private final boolean explicitOff;
    private final boolean explicitAuto;
    private final boolean sparseEnabled;

    /**
     * @param schemaVersion the schema version number of the HLL. This must
     *        be greater than or equal to zero.
     * @param type the {@link HLLType type} of the HLL. This cannot
     *        be <code>null</code>.
     * @param registerCountLog2 the log-base-2 register count parameter for
     *        probabilistic HLLs. This must be greater than or equal to zero.
     * @param registerWidth the register width parameter for probabilistic
     *        HLLs. This must be greater than or equal to zero.
     * @param log2ExplicitCutoff the log-base-2 of the explicit cardinality cutoff,
     *        if it is explicitly defined. (If <code>explicitOff</code> or
     *        <code>explicitAuto</code> is <code>true</code> then this has no
     *        meaning.)
     * @param explicitOff the flag for 'explicit off'-mode, where the
     *        {@link HLLType#EXPLICIT} representation is not used. Both this and
     *        <code>explicitAuto</code> cannot be <code>true</code> at the same
     *        time.
     * @param explicitAuto the flag for 'explicit auto'-mode, where the
     *        {@link HLLType#EXPLICIT} representation's promotion cutoff is
     *        determined based on in-memory size automatically. Both this and
     *        <code>explicitOff</code> cannot be <code>true</code> at the same
     *        time.
     * @param sparseEnabled the flag for 'sparse-enabled'-mode, where the
     *        {@link HLLType#SPARSE} representation is used.
     */
    public HLLMetadata(final int schemaVersion,
                       final HLLType type,
                       final int registerCountLog2,
                       final int registerWidth,
                       final int log2ExplicitCutoff,
                       final boolean explicitOff,
                       final boolean explicitAuto,
                       final boolean sparseEnabled) {
        this.schemaVersion = schemaVersion;
        this.type = type;
        this.registerCountLog2 = registerCountLog2;
        this.registerWidth = registerWidth;
        this.log2ExplicitCutoff = log2ExplicitCutoff;
        this.explicitOff = explicitOff;
        this.explicitAuto = explicitAuto;
        this.sparseEnabled = sparseEnabled;
    }

    /* (non-Javadoc)
     * @see net.agkn.hll.serialization.IHLLMetadata#schemaVersion()
     */
    @Override
    public int schemaVersion() { return schemaVersion; }

    /* (non-Javadoc)
     * @see net.agkn.hll.serialization.IHLLMetadata#HLLType()
     */
    @Override
    public HLLType HLLType() { return type; }

    /* (non-Javadoc)
     * @see net.agkn.hll.serialization.IHLLMetadata#registerCountLog2()
     */
    @Override
    public int registerCountLog2() { return registerCountLog2; }

    /* (non-Javadoc)
     * @see net.agkn.hll.serialization.IHLLMetadata#registerWidth()
     */
    @Override
    public int registerWidth() { return registerWidth; }

    /* (non-Javadoc)
     * @see net.agkn.hll.serialization.IHLLMetadata#log2ExplicitCutoff()
     */
    @Override
    public int log2ExplicitCutoff() { return log2ExplicitCutoff; }

    /* (non-Javadoc)
     * @see net.agkn.hll.serialization.IHLLMetadata#explicitOff()
     */
    @Override
    public boolean explicitOff() {
        return explicitOff;
    }

    /* (non-Javadoc)
     * @see net.agkn.hll.serialization.IHLLMetadata#explicitAuto()
     * @see net.agkn.hll.serialization.IHLLMetadata#log2ExplicitCutoff()
     */
    @Override
    public boolean explicitAuto() {
        return explicitAuto;
    }

    /* (non-Javadoc)
     * @see net.agkn.hll.serialization.IHLLMetadata#sparseEnabled()
     */
    @Override
    public boolean sparseEnabled() { return sparseEnabled; }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "<HLLMetadata schemaVersion: " + this.schemaVersion + ", type: " + this.type.toString() + ", registerCountLog2: " + this.registerCountLog2 + ", registerWidth: " + this.registerWidth + ", log2ExplicitCutoff: " + this.log2ExplicitCutoff + ", explicitOff: " + this.explicitOff + ", explicitAuto: " +this.explicitAuto + ">";
    }
}

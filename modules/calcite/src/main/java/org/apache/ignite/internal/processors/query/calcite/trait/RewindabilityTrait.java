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

package org.apache.ignite.internal.processors.query.calcite.trait;

import org.apache.calcite.plan.RelMultipleTrait;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;
import org.jetbrains.annotations.NotNull;

/** */
public class RewindabilityTrait implements RelMultipleTrait {
    /** */
    public static final RewindabilityTrait ONE_WAY = canonize(new RewindabilityTrait(false));

    /** */
    public static final RewindabilityTrait REWINDABLE = canonize(new RewindabilityTrait(true));

    /** */
    private final boolean rewindable;

    /** */
    private RewindabilityTrait(boolean rewindable) {
        this.rewindable = rewindable;
    }

    /** */
    public boolean rewindable() {
        return rewindable;
    }

    /** {@inheritDoc} */
    @Override public boolean isTop() {
        return !rewindable();
    }

    /** {@inheritDoc} */
    @Override public int compareTo(@NotNull RelMultipleTrait o) {
        RewindabilityTrait that = (RewindabilityTrait)o;
        return Boolean.compare(that.rewindable, rewindable);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (!(o instanceof RewindabilityTrait))
            return false;
        return compareTo((RewindabilityTrait)o) == 0;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return (rewindable ? 1 : 0);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return rewindable ? "rewindable" : "one-way";
    }

    /** {@inheritDoc} */
    @Override public RelTraitDef<RewindabilityTrait> getTraitDef() {
        return RewindabilityTraitDef.INSTANCE;
    }

    /** {@inheritDoc} */
    @Override public boolean satisfies(RelTrait trait) {
        if (this == trait)
            return true;

        if (!(trait instanceof RewindabilityTrait))
            return false;

        RewindabilityTrait trait0 = (RewindabilityTrait)trait;

        return !trait0.rewindable() || rewindable();
    }

    /** {@inheritDoc} */
    @Override public void register(RelOptPlanner planner) {
        // no-op
    }

    /** */
    private static RewindabilityTrait canonize(RewindabilityTrait trait) {
        return RewindabilityTraitDef.INSTANCE.canonize(trait);
    }
}

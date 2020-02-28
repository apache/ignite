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

package org.apache.ignite.internal.processors.query.calcite.serialize;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.processors.query.calcite.metadata.NodesMapping;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSender;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionFunction;

/**
 * Describes {@link IgniteSender}.
 */
public class SenderPhysicalRel implements PhysicalRel {
    /** */
    private long targetFragmentId;

    /** */
    private NodesMapping mapping;

    /** */
    private DistributionFunction function;

    /** */
    private ImmutableIntList distributionKeys;

    /** */
    private PhysicalRel input;

    public SenderPhysicalRel() {
    }

    /** */
    public SenderPhysicalRel(long targetFragmentId, NodesMapping mapping, DistributionFunction function, ImmutableIntList distributionKeys, PhysicalRel input) {
        this.targetFragmentId = targetFragmentId;
        this.mapping = mapping;
        this.function = function;
        this.distributionKeys = distributionKeys;
        this.input = input;
    }

    public long targetFragmentId() {
        return targetFragmentId;
    }

    public NodesMapping mapping() {
        return mapping;
    }

    public DistributionFunction distributionFunction() {
        return function;
    }

    public ImmutableIntList distributionKeys() {
        return distributionKeys;
    }

    public PhysicalRel input() {
        return input;
    }

    @Override public <T> T accept(PhysicalRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(targetFragmentId);
        out.writeObject(mapping);
        out.writeObject(function);
        out.writeObject(distributionKeys.toIntArray());
        out.writeObject(input);
    }

    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        targetFragmentId = in.readLong();
        mapping = (NodesMapping) in.readObject();
        function = (DistributionFunction) in.readObject();
        distributionKeys = ImmutableIntList.of((int[]) in.readObject());
        input = (PhysicalRel) in.readObject();
    }
}

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

package org.apache.ignite.internal.management;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;
import lombok.Data;
import org.apache.ignite.internal.management.api.CommandWithSubs;
import org.apache.ignite.internal.management.api.Parameter;
import org.apache.ignite.internal.management.api.PositionalParameter;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
@Data
public class MetricCommand extends CommandWithSubs {
    /** */
    private static final long serialVersionUID = 0;

    /** Metric name. */
    @PositionalParameter(description = "Name of the metric which value should be printed. " +
        "If name of the metric registry is specified, value of all its metrics will be printed")
    private String name;

    /** */
    @Parameter(
        description = "ID of the node to get the metric values from. If not set, random node will be chosen",
        optional = true
    )
    private UUID nodeId;

    /** */
    public MetricCommand() {
        register(MetricConfigureHistogramCommand::new);
        register(MetricConfigureHitrateCommand::new);
    }

    /** {@inheritDoc} */
    @Override public String description() {
        return "Print metric value";
    }

    /** {@inheritDoc} */
    @Override public boolean canBeExecuted() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean positionalSubsName() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        super.writeExternalData(out);

        U.writeString(out, name);
        U.writeUuid(out, nodeId);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternalData(protoVer, in);

        name = U.readString(in);
        nodeId = U.readUuid(in);
    }
}

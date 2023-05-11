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

package org.apache.ignite.internal.management.baseline;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.Argument;
import org.apache.ignite.internal.management.api.CliPositionalSubcommands;
import org.apache.ignite.internal.management.api.CommandRegistryImpl;
import org.apache.ignite.internal.management.api.ComputeCommand;
import org.apache.ignite.internal.management.baseline.BaselineCommand.VisorBaselineTaskArg;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.visor.baseline.VisorBaselineTask;
import org.apache.ignite.internal.visor.baseline.VisorBaselineTaskResult;
import static org.apache.ignite.internal.management.api.CommandUtils.coordinatorOrNull;

/** */
@CliPositionalSubcommands
public class BaselineCommand extends CommandRegistryImpl<VisorBaselineTaskArg, VisorBaselineTaskResult>
    implements ComputeCommand<VisorBaselineTaskArg, VisorBaselineTaskResult> {
    /** {@inheritDoc} */
    @Override public String description() {
        return "Print cluster baseline topology";
    }

    /** */
    public BaselineCommand() {
        super(
            new BaselineAddCommand(),
            new BaselineRemoveCommand(),
            new BaselineSetCommand(),
            new BaselineVersionCommand(),
            new BaselineAutoAdjustCommand()
        );
    }

    /** {@inheritDoc} */
    @Override public Class<BaselineCommandArg> argClass() {
        return BaselineCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<VisorBaselineTask> taskClass() {
        return VisorBaselineTask.class;
    }

    /** {@inheritDoc} */
    @Override public Collection<UUID> nodes(Map<UUID, T3<Boolean, Object, Long>> nodes, VisorBaselineTaskArg arg) {
        return coordinatorOrNull(nodes);
    }

    /** {@inheritDoc} */
    @Override public void printResult(VisorBaselineTaskArg arg, VisorBaselineTaskResult res, Consumer<String> printer) {
        new AbstractBaselineCommand() {
            @Override public String description() {
                return null;
            }

            @Override public Class<? extends VisorBaselineTaskArg> argClass() {
                return null;
            }
        }.printResult(arg, res, printer);
    }

    /** */
    public abstract static class VisorBaselineTaskArg extends IgniteDataTransferObject {
        /** */
        @Argument(optional = true, description = "Show the full list of node ips")
        private boolean verbose;

        /** {@inheritDoc} */
        @Override protected void writeExternalData(ObjectOutput out) throws IOException {
            out.writeBoolean(verbose);
        }

        /** {@inheritDoc} */
        @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
            verbose = in.readBoolean();
        }

        /** */
        public boolean verbose() {
            return verbose;
        }

        /** */
        public void verbose(boolean verbose) {
            this.verbose = verbose;
        }
    }
}

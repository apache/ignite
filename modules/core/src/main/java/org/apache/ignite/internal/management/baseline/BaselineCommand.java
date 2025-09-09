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
import java.util.function.Consumer;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.Argument;
import org.apache.ignite.internal.management.api.CommandRegistryImpl;
import org.apache.ignite.internal.management.api.ComputeCommand;
import org.apache.ignite.internal.management.baseline.BaselineCommand.BaselineTaskArg;

import static org.apache.ignite.internal.management.api.CommandUtils.coordinatorOrNull;

/** */
public class BaselineCommand extends CommandRegistryImpl<BaselineTaskArg, BaselineTaskResult>
    implements ComputeCommand<BaselineTaskArg, BaselineTaskResult> {
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
    @Override public Class<BaselineTask> taskClass() {
        return BaselineTask.class;
    }

    /** {@inheritDoc} */
    @Override public Collection<ClusterNode> nodes(Collection<ClusterNode> nodes, BaselineTaskArg arg) {
        return coordinatorOrNull(nodes);
    }

    /** {@inheritDoc} */
    @Override public void printResult(BaselineTaskArg arg, BaselineTaskResult res, Consumer<String> printer) {
        new AbstractBaselineCommand() {
            @Override public String description() {
                return null;
            }

            @Override public Class<? extends BaselineTaskArg> argClass() {
                return null;
            }
        }.printResult(arg, res, printer);
    }

    /** */
    public abstract static class BaselineTaskArg extends IgniteDataTransferObject {
        /** */
        @Argument(optional = true, description = "Show the full list of node ips")
        private boolean verbose;

        /** {@inheritDoc} */
        @Override protected void writeExternalData(ObjectOutput out) throws IOException {
            out.writeBoolean(verbose);
        }

        /** {@inheritDoc} */
        @Override protected void readExternalData(ObjectInput in) throws IOException, ClassNotFoundException {
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

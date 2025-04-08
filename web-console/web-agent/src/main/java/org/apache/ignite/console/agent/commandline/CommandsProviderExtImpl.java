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

package org.apache.ignite.console.agent.commandline;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.console.agent.IgniteClusterLauncher;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.commandline.CommandsProvider;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.Argument;
import org.apache.ignite.internal.management.api.Command;
import org.apache.ignite.internal.management.api.LocalCommand;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Additional commands provider for control utility.
 */
public class CommandsProviderExtImpl implements CommandsProvider {
    /** */
    public static final Command<?, ?> NODE_START_COMMAND = new NodeStartCommand();
    public static final Command<?, ?> NODE_STOP_COMMAND = new NodeStopCommand();


    /** {@inheritDoc} */
    @Override public Collection<Command<?, ?>> commands() {
        return List.of(NODE_START_COMMAND,NODE_STOP_COMMAND);
    }

    /** */
    public static class NodeStartCommand implements LocalCommand<NodeStartCommandArg, Boolean> {
        /** {@inheritDoc} */
        @Override public String description() {
            return "在当前Ignite进程内新启动一个名称为输入参数的Instance";
        }

        /** {@inheritDoc} */
        @Override public Class<NodeStartCommandArg> argClass() {
            return NodeStartCommandArg.class;
        }

        /** {@inheritDoc} */
        @Override public Boolean execute(@Nullable IgniteClient client, Ignite ignite0, NodeStartCommandArg arg, Consumer<String> printer) {
            printer.accept("Start Node: "+ arg.instanceName() + ", cfg: " + arg.cfgPath());            
            boolean isLastNode = !F.isEmpty(arg.clusterId());
			// 启动一个独立的node，jvm内部的node之间相互隔离
			Ignite ignite;
			try {
				ignite = IgniteClusterLauncher.trySingleStart(arg.clusterId(), arg.instanceName(), 0, isLastNode, arg.cfgPath());
				if(ignite!=null && isLastNode) {
					IgniteClusterLauncher.registerNodeUrl(ignite,arg.clusterId());
					
					IgniteClusterLauncher.deployServices(ignite.services(ignite.cluster().forServers()));
		        	return true;
				}
				else {
					return false;
				}
			} catch (IgniteCheckedException e) {
				printer.accept(e.getMessage());
				return false;
			}
        }
    }

    /** */
    public static class NodeStartCommandArg extends IgniteDataTransferObject {
        /** */
        private static final long serialVersionUID = 0;

        /** */
        @Argument
        private String instanceName;
        
        @Argument(optional=true)
        private String clusterId; // UUID
        
        @Argument
        private String cfgPath;


        /** {@inheritDoc} */
        @Override protected void writeExternalData(ObjectOutput out) throws IOException {
            U.writeString(out, instanceName);
            U.writeString(out, clusterId);
            U.writeString(out, cfgPath);
        }

        /** {@inheritDoc} */
        @Override protected void readExternalData(ObjectInput in) throws IOException {
        	instanceName = U.readString(in);
        	clusterId = U.readString(in);
        	cfgPath = U.readString(in);
        }

        /** */
        public void instanceName(String instanceName) {
            this.instanceName = instanceName;
        }

        /** */
        public String instanceName() {
            return instanceName;
        }
        
        /** */
        public void clusterId(String clusterId) {
            this.clusterId = clusterId;
        }

        /** */
        public String clusterId() {
            return clusterId;
        }
        
        /** */
        public void cfgPath(String cfgPath) {
            this.cfgPath = cfgPath;
        }

        /** */
        public String cfgPath() {
            return cfgPath;
        }
    }
    
    /** */
    public static class NodeStopCommand implements LocalCommand<NodeStopCommandArg, Boolean> {
        /** {@inheritDoc} */
        @Override public String description() {
            return "在当前Ignite进程内新关闭一个名称为输入参数的Instance";
        }

        /** {@inheritDoc} */
        @Override public Class<NodeStopCommandArg> argClass() {
            return NodeStopCommandArg.class;
        }

        /** {@inheritDoc} */
        @Override public Boolean execute(@Nullable IgniteClient client, Ignite ignite, NodeStopCommandArg arg, Consumer<String> printer) {
            printer.accept("Stop Node: "+ arg.instanceName() + ", clusterId: " + arg.clusterId());
            boolean isLastNode = !F.isEmpty(arg.clusterId());
            IgniteClusterLauncher.stopIgnite(arg.instanceName(),arg.clusterId());

            return true;
        }
    }

    /** */
    public static class NodeStopCommandArg extends IgniteDataTransferObject {
        /** */
        private static final long serialVersionUID = 0;

        /** */
        @Argument
        private String instanceName;
        
        @Argument(optional=true)
        private String clusterId; // UUID

        /** {@inheritDoc} */
        @Override protected void writeExternalData(ObjectOutput out) throws IOException {
            U.writeString(out, instanceName);
            U.writeString(out, clusterId);
        }

        /** {@inheritDoc} */
        @Override protected void readExternalData(ObjectInput in) throws IOException {
        	instanceName = U.readString(in);
        	clusterId = U.readString(in);
        }

        /** */
        public void instanceName(String instanceName) {
            this.instanceName = instanceName;
        }

        /** */
        public String instanceName() {
            return instanceName;
        }
        
        /** */
        public void clusterId(String clusterId) {
            this.clusterId = clusterId;
        }

        /** */
        public String clusterId() {
            return clusterId;
        }
    }
}

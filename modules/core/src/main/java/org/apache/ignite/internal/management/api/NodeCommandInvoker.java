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

package org.apache.ignite.internal.management.api;

import org.apache.ignite.Ignite;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;

/** */
public class NodeCommandInvoker<A extends IgniteDataTransferObject> extends AbstractCommandInvoker<A> {
    /** */
    private final IgniteEx ignite;

    /**
     * @param cmd Command to execute.
     * @param arg Argument.
     * @param ignite Ignite node.
     */
    public NodeCommandInvoker(Command<A, ?> cmd, A arg, IgniteEx ignite) {
        super(cmd, arg);
        this.ignite = ignite;
    }

    /** {@inheritDoc} */
    @Override protected GridClient client() throws GridClientException {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected Ignite ignite() {
        return ignite;
    }

    /** {@inheritDoc} */
    @Override protected GridClientNode defaultNode() {
        return CommandUtils.clusterToClientNode(ignite.localNode());
    }
}

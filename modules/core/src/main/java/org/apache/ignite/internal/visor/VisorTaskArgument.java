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

package org.apache.ignite.internal.visor;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Visor tasks argument.
 */
public class VisorTaskArgument<A> extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Node IDs task should be mapped to. */
    private List<UUID> nodes;

    /** Task argument. */
    private A arg;

    /** Debug flag. */
    private boolean debug;

    /**
     * Default constructor.
     */
    public VisorTaskArgument() {
        // No-op.
    }

    /**
     * Create Visor task argument.
     *
     * @param nodes Node IDs task should be mapped to.
     * @param arg Task argument.
     * @param debug Debug flag.
     */
    public VisorTaskArgument(Collection<UUID> nodes, A arg, boolean debug) {
        assert nodes != null;
        assert !nodes.isEmpty();

        this.nodes = toList(nodes);
        this.arg = arg;
        this.debug = debug;
    }

    /**
     * Create Visor task argument with nodes, but without actual argument.
     *
     * @param nodes Node IDs task should be mapped to.
     * @param debug Debug flag.
     */
    public VisorTaskArgument(Collection<UUID> nodes, boolean debug) {
        this(nodes, null, debug);
    }

    /**
     * Create Visor task argument.
     *
     * @param node Node ID task should be mapped to.
     * @param arg Task argument.
     * @param debug Debug flag.
     */
    public VisorTaskArgument(UUID node, A arg, boolean debug) {
        this(Collections.singletonList(node), arg, debug);
    }

    /**
     * Create Visor task argument with nodes, but without actual argument.
     *
     * @param node Node ID task should be mapped to.
     * @param debug Debug flag.
     */
    public VisorTaskArgument(UUID node, boolean debug) {
        this(node, null, debug);
    }

    /**
     * @return Node IDs task should be mapped to.
     */
    public List<UUID> getNodes() {
        return nodes;
    }

    /**
     * @return Task argument.
     */
    public A getArgument() {
        return arg;
    }

    /**
     * @return Debug flag.
     */
    public boolean isDebug() {
        return debug;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeCollection(out, nodes);
        out.writeObject(arg);
        out.writeBoolean(debug);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        nodes = U.readList(in);
        arg = (A)in.readObject();
        debug = in.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorTaskArgument.class, this);
    }
}

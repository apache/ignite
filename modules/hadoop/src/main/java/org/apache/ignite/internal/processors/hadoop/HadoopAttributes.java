/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.hadoop;

import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.HadoopConfiguration;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;

/**
 * Hadoop attributes.
 */
public class HadoopAttributes implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Attribute name. */
    public static final String NAME = IgniteNodeAttributes.ATTR_PREFIX + ".hadoop";

    /** Map-reduce planner class name. */
    private String plannerCls;

    /** External executor flag. */
    private boolean extExec;

    /** Maximum parallel tasks. */
    private int maxParallelTasks;

    /** Maximum task queue size. */
    private int maxTaskQueueSize;

    /** Library names. */
    @GridToStringExclude
    private String[] libNames;

    /** Number of cores. */
    private int cores;

    /**
     * Get attributes for node (if any).
     *
     * @param node Node.
     * @return Attributes or {@code null} if Hadoop Accelerator is not enabled for node.
     */
    @Nullable public static HadoopAttributes forNode(ClusterNode node) {
        return node.attribute(NAME);
    }

    /**
     * {@link Externalizable} support.
     */
    public HadoopAttributes() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param cfg Configuration.
     */
    public HadoopAttributes(HadoopConfiguration cfg) {
        assert cfg != null;
        assert cfg.getMapReducePlanner() != null;

        plannerCls = cfg.getMapReducePlanner().getClass().getName();

        // TODO: IGNITE-404: Get from configuration when fixed.
        extExec = false;

        maxParallelTasks = cfg.getMaxParallelTasks();
        maxTaskQueueSize = cfg.getMaxTaskQueueSize();
        libNames = cfg.getNativeLibraryNames();

        // Cores count already passed in other attributes, we add it here for convenience.
        cores = Runtime.getRuntime().availableProcessors();
    }

    /**
     * @return Map reduce planner class name.
     */
    public String plannerClassName() {
        return plannerCls;
    }

    /**
     * @return External execution flag.
     */
    public boolean externalExecution() {
        return extExec;
    }

    /**
     * @return Maximum parallel tasks.
     */
    public int maxParallelTasks() {
        return maxParallelTasks;
    }

    /**
     * @return Maximum task queue size.
     */
    public int maxTaskQueueSize() {
        return maxTaskQueueSize;
    }


    /**
     * @return Native library names.
     */
    public String[] nativeLibraryNames() {
        return libNames;
    }

    /**
     * @return Number of cores on machine.
     */
    public int cores() {
        return cores;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(plannerCls);
        out.writeBoolean(extExec);
        out.writeInt(maxParallelTasks);
        out.writeInt(maxTaskQueueSize);
        out.writeObject(libNames);
        out.writeInt(cores);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        plannerCls = (String)in.readObject();
        extExec = in.readBoolean();
        maxParallelTasks = in.readInt();
        maxTaskQueueSize = in.readInt();
        libNames = (String[])in.readObject();
        cores = in.readInt();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(HadoopAttributes.class, this, "libNames", Arrays.toString(libNames));
    }
}

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

package org.apache.ignite.internal.classpath;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;

import static org.apache.ignite.internal.classpath.IgniteClassPathState.READY;

/**
 * Class path POJO.
 */
public class IgniteClassPath implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private UUID id;

    /** */
    private Set<UUID> deployedOnNodes;

    /** */
    private String name;

    /** */
    private String[] files;

    /** */
    private long[] lengths;

    /** */
    private IgniteClassPathState state;

    /**
     * @param id Unique id of classpath.
     * @param name User provided name.
     * @param files Files to include to classpath.
     */
    public IgniteClassPath(
        UUID id,
        Set<UUID> deployedOnNodes,
        String name,
        String[] files,
        long[] lengths,
        IgniteClassPathState state
    ) {
        assert files.length == lengths.length;

        this.id = id;
        this.deployedOnNodes = deployedOnNodes;
        this.name = name;
        this.files = files;
        this.lengths = lengths;
        this.state = state;
    }

    /** */
    public static IgniteClassPath fromProperties(Properties p, UUID locNodeId) {
        int sz = 0;

        while (p.containsKey("files." + sz))
            sz++;

        String[] files = new String[sz];
        long[] lengths = new long[sz];

        for (int i = 0; i < sz; i++) {
            files[i] = p.getProperty("files." + i);
            lengths[i] = Long.parseLong(p.getProperty("length." + i));
        }

        return new IgniteClassPath(
            UUID.fromString(p.getProperty("id")),
            Set.of(locNodeId),
            p.getProperty("name"),
            files,
            lengths,
            IgniteClassPathState.valueOf(p.getProperty("state"))
        );
    }

    /** */
    public Properties toProperties() {
        Properties p = new Properties();

        p.setProperty("id", id.toString());
        p.setProperty("name", name);
        p.setProperty("state", state.name());

        for (int i = 0; i < files.length; i++) {
            p.setProperty("files." + i, files[i]);
            p.setProperty("length." + i, Long.toString(lengths[i]));
        }

        return p;
    }

    /**
     * @return {@code True} if {@code this} and {@code that} instances has all fields equals except {@link #deployedOnNodes}.
     */
    public boolean equalsWithoutNodes(IgniteClassPath that) {
        return equals(that, false);
    }

    /** */
    private boolean equals(IgniteClassPath that, boolean includeNodes) {
        if (that == null)
            return false;

        return Objects.equals(id, that.id)
            && (!includeNodes || Objects.equals(deployedOnNodes, that.deployedOnNodes))
            && Objects.equals(name, that.name)
            && Objects.deepEquals(files, that.files)
            && Objects.deepEquals(lengths, that.lengths)
            && state == that.state;
    }

    /**
     * @param state New state.
     */
    IgniteClassPath newState(IgniteClassPathState state) {
        if (this.state == state)
            return this;

        return new IgniteClassPath(id, deployedOnNodes, name, files, lengths, state);
    }

    /**
     * Adds {@code node} to {@link #deployedOnNodes()} set and returns new instance.
     * @param node Node to add.
     * @return Instance with modified set.
     */
    IgniteClassPath addDeployedOnNode(UUID node) {
        if (deployedOnNodes.contains(node))
            return this;

        Set<UUID> deployedOnNodes0 = new HashSet<>(deployedOnNodes);

        deployedOnNodes0.add(node);

        IgniteClassPathState newState = state == IgniteClassPathState.LOST ? READY : state;

        return new IgniteClassPath(id, deployedOnNodes0, name, files, lengths, newState);
    }

    /**
     * Removes {@code node} from {@link #deployedOnNodes()} set and returns new instance.
     * @param node Node to add.
     * @return Instance with modified set.
     */
    public IgniteClassPath removeDeployedOnNode(UUID node) {
        if (!deployedOnNodes.contains(node))
            return this;

        Set<UUID> deployedOnNodes0 = new HashSet<>(deployedOnNodes);

        deployedOnNodes0.remove(node);

        IgniteClassPathState newState = (state == READY && deployedOnNodes0.isEmpty()) ? IgniteClassPathState.LOST : state;

        return new IgniteClassPath(id, deployedOnNodes0, name, files, lengths, newState);
    }

    /** */
    public IgniteClassPathState state() {
        return state;
    }

    /** */
    public UUID id() {
        return id;
    }

    /** */
    public Set<UUID> deployedOnNodes() {
        return deployedOnNodes;
    }

    /** */
    public String name() {
        return name;
    }

    /** */
    public String[] files() {
        return files;
    }

    /** */
    public long[] lengths() {
        return lengths;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteClassPath.class, this);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        IgniteClassPath that = (IgniteClassPath)o;
        return equals(that, true);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(id, deployedOnNodes, name, Arrays.hashCode(files), Arrays.hashCode(lengths), state);
    }
}

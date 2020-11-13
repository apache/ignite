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

package org.apache.ignite.internal.processors.query.calcite.prepare;

import java.util.List;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationMappingException;
import org.apache.ignite.internal.processors.query.calcite.metadata.FragmentMapping;
import org.apache.ignite.internal.processors.query.calcite.metadata.FragmentMappingException;
import org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMdFragmentMapping;
import org.apache.ignite.internal.processors.query.calcite.metadata.NodeMappingException;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteReceiver;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSender;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.query.calcite.externalize.RelJsonWriter.toJson;

/**
 * Fragment of distributed query
 */
public class Fragment {
    /** */
    private final long id;

    /** */
    private final IgniteRel root;

    /** Serialized root representation. */
    private String rootSer;

    /** */
    private final ImmutableList<IgniteReceiver> remotes;

    /**
     * @param id Fragment id.
     * @param root Root node of the fragment.
     * @param remotes Remote sources of the fragment.
     */
    public Fragment(long id, IgniteRel root, List<IgniteReceiver> remotes) {
        this(id, root, remotes, null);
    }

    /**
     * @param id Fragment id.
     * @param root Root node of the fragment.
     * @param remotes Remote sources of the fragment.
     * @param rootSer Root serialized representation.
     */
    public Fragment(long id, IgniteRel root, List<IgniteReceiver> remotes, @Nullable String rootSer) {
        this.id = id;
        this.root = root;
        this.remotes = ImmutableList.copyOf(remotes);
        this.rootSer = rootSer;
    }

    /**
     * Mapps the fragment to its data location.
     * @param ctx Planner context.
     * @param mq Metadata query.
     */
    FragmentMapping map(PlanningContext ctx, RelMetadataQuery mq) throws FragmentMappingException {
        try {
            FragmentMapping mapping = IgniteMdFragmentMapping._fragmentMapping(root, mq);

            return rootFragment() ? FragmentMapping.create(ctx.localNodeId()).colocate(mapping) : mapping;
        }
        catch (NodeMappingException e) {
            throw new FragmentMappingException("Failed to calculate physical distribution", this, e.node(), e);
        }
        catch (ColocationMappingException e) {
            throw new FragmentMappingException("Failed to calculate physical distribution", this, root, e);
        }
    }

    /**
     * @return Fragment ID.
     */
    public long fragmentId() {
        return id;
    }

    /**
     * @return Root node.
     */
    public IgniteRel root() {
        return root;
    }

    /**
     * Lazy serialized root representation.
     *
     * @return Serialized form.
     */
    public String serialized() {
        return rootSer != null ? rootSer : (rootSer = toJson(root()));
    }

    /**
     * @return Fragment remote sources.
     */
    public List<IgniteReceiver> remotes() {
        return remotes;
    }

    /** */
    public boolean rootFragment() {
        return !(root instanceof IgniteSender);
    }
}

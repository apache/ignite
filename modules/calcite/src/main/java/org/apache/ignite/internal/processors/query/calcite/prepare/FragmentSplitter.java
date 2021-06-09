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

import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteReceiver;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSender;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTrimExchange;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

/**
 *
 */
public class FragmentSplitter extends IgniteRelShuttle {
    /** */
    private final Deque<FragmentProto> stack = new LinkedList<>();

    /** */
    private RelNode cutPoint;

    /** */
    private FragmentProto curr;

    /** */
    public FragmentSplitter(RelNode cutPoint) {
        this.cutPoint = cutPoint;
    }

    /** */
    public List<Fragment> go(Fragment fragment) {
        ArrayList<Fragment> res = new ArrayList<>();

        stack.push(new FragmentProto(IdGenerator.nextId(), fragment.root()));

        while (!stack.isEmpty()) {
            curr = stack.pop();
            curr.root = visit(curr.root);
            res.add(curr.build());
            curr = null;
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteReceiver rel) {
        curr.remotes.add(rel);

        return rel;
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteExchange rel) {
        throw new AssertionError();
    }

    /**
     * Visits all children of a parent.
     */
    @Override protected IgniteRel processNode(IgniteRel rel) {
        if (rel == cutPoint) {
            cutPoint = null;

            return split(rel);
        }

        List<IgniteRel> inputs = Commons.cast(rel.getInputs());

        for (int i = 0; i < inputs.size(); i++)
            visitChild(rel, i, inputs.get(i));

        return rel;
    }

    /** */
    private IgniteRel split(IgniteRel rel) {
        RelOptCluster cluster = rel.getCluster();
        RelTraitSet traits = rel.getTraitSet();
        RelDataType rowType = rel.getRowType();

        RelNode input = rel instanceof IgniteTrimExchange ? rel.getInput(0) : rel;

        long targetFragmentId = curr.id;
        long sourceFragmentId = IdGenerator.nextId();
        long exchangeId = sourceFragmentId;

        IgniteReceiver receiver = new IgniteReceiver(cluster, traits, rowType, exchangeId, sourceFragmentId);
        IgniteSender sender = new IgniteSender(cluster, traits, input, exchangeId, targetFragmentId, rel.distribution());

        curr.remotes.add(receiver);
        stack.push(new FragmentProto(sourceFragmentId, sender));

        return receiver;
    }

    /** */
    private static class FragmentProto {
        /** */
        private final long id;

        /** */
        private IgniteRel root;

        /** */
        private final ImmutableList.Builder<IgniteReceiver> remotes = ImmutableList.builder();

        /** */
        private FragmentProto(long id, IgniteRel root) {
            this.id = id;
            this.root = root;
        }

        /** */
        Fragment build() {
            return new Fragment(id, root, remotes.build());
        }
    }
}

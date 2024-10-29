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
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteReceiver;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSender;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTrimExchange;
import org.jetbrains.annotations.Nullable;

/**
 * Splits a query into a list of query fragments.
 */
public class Splitter extends IgniteRelShuttle {
    /** */
    private final Deque<FragmentProto> stack = new LinkedList<>();

    /** */
    private @Nullable FragmentProto curr;

    /** */
    public List<Fragment> go(IgniteRel root) {
        ArrayList<Fragment> res = new ArrayList<>();

        stack.push(new FragmentProto(IdGenerator.nextId(), root));

        while (!stack.isEmpty()) {
            curr = stack.pop();

            // We need to clone it after CALCITE-5503, otherwise it become possible to obtain equals multiple inputs i.e.:
            //          rel#348IgniteExchange
            //          rel#287IgniteMergeJoin
            //       _____|             |_____
            //       V                       V
            //   IgniteSort#285            IgniteSort#285
            //   IgniteTableScan#180       IgniteTableScan#180
            curr.root = Cloner.clone(curr.root);

            curr.root = visit(curr.root);

            res.add(curr.build());

            curr = null;
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteReceiver rel) {
        throw new AssertionError();
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteExchange rel) {
        RelOptCluster cluster = rel.getCluster();

        long targetFragmentId = curr.id;
        long srcFragmentId = IdGenerator.nextId();
        long exchangeId = srcFragmentId;

        IgniteReceiver receiver = new IgniteReceiver(cluster, rel.getTraitSet(), rel.getRowType(), exchangeId, srcFragmentId);
        IgniteSender snd = new IgniteSender(cluster, rel.getTraitSet(), rel.getInput(), exchangeId, targetFragmentId,
            rel.distribution());

        curr.remotes.add(receiver);
        stack.push(new FragmentProto(srcFragmentId, snd));

        return receiver;
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteTrimExchange rel) {
        return ((IgniteTrimExchange)processNode(rel)).clone(IdGenerator.nextId());
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteIndexScan rel) {
        return rel.clone(IdGenerator.nextId());
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteTableScan rel) {
        return rel.clone(IdGenerator.nextId());
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

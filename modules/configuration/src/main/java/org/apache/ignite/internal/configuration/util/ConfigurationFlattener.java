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

package org.apache.ignite.internal.configuration.util;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.ignite.internal.configuration.SuperRoot;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.apache.ignite.internal.configuration.tree.NamedListNode;

/** Utility class that has {@link ConfigurationFlattener#createFlattenedUpdatesMap(SuperRoot, SuperRoot)} method. */
public class ConfigurationFlattener {
    /**
     * Convert a traversable tree to a map of qualified keys to values.
     *
     * @param curRoot Current root tree.
     * @param updates Tree with updates.
     * @return Map of changes.
     */
    public static Map<String, Serializable> createFlattenedUpdatesMap(SuperRoot curRoot, SuperRoot updates) {
        // Resulting map.
        Map<String, Serializable> resMap = new HashMap<>();

        // This method traverses two trees at the same time. In order to reuse the visitor object it's decided to
        // use an explicit stack for nodes of the "old" tree. We need to reuse the visitor object because it accumulates
        // "full" keys in dot-separated notation. Please refer to KeysTrackingConfigurationVisitor for details..
        Deque<InnerNode> oldInnerNodesStack = new ArrayDeque<>();

        oldInnerNodesStack.push(curRoot);

        // Explicit access to the children of super root guarantees that "oldInnerNodesStack" is never empty and thus
        // we don't need null-checks when calling Deque#peek().
        updates.traverseChildren(new FlattenerVisitor(oldInnerNodesStack, resMap));

        assert oldInnerNodesStack.peek() == curRoot : oldInnerNodesStack;

        return resMap;
    }

    /**
     * @param node Named list node.
     * @return Map that contains same keys and their positions as values.
     */
    private static Map<String, Integer> keysToOrderIdx(NamedListNode<?> node) {
        Map<String, Integer> res = new HashMap<>();

        int idx = 0;

        for (String key : node.namedListKeys()) {
            if (node.get(key) != null)
                res.put(key, idx++);
        }

        return res;
    }

    /**
     * Visitor that collects diff between "old" and "new" trees into a flat map.
     */
    private static class FlattenerVisitor extends KeysTrackingConfigurationVisitor<Object> {
        /** Old nodes stack for recursion. */
        private final Deque<InnerNode> oldInnerNodesStack;

        /** Map with the result. */
        private final Map<String, Serializable> resMap;

        /** Flag indicates that "old" and "new" trees are literally the same at the moment. */
        private boolean singleTreeTraversal;

        /**
         * Makes sense only if {@link #singleTreeTraversal} is {@code true}. Helps distinguishing creation from
         * deletion. Always {@code false} if {@link #singleTreeTraversal} is {@code false}.
         */
        private boolean deletion;

        FlattenerVisitor(Deque<InnerNode> oldInnerNodesStack, Map<String, Serializable> resMap) {
            this.oldInnerNodesStack = oldInnerNodesStack;
            this.resMap = resMap;
        }

        /** {@inheritDoc} */
        @Override public Void doVisitLeafNode(String key, Serializable newVal) {
            // Read same value from old tree.
            Serializable oldVal = oldInnerNodesStack.peek().traverseChild(key, ConfigurationUtil.leafNodeVisitor());

            // Do not put duplicates into the resulting map.
            if (singleTreeTraversal || !Objects.deepEquals(oldVal, newVal))
                resMap.put(currentKey(), deletion ? null : newVal);

            return null;
        }

        /** {@inheritDoc} */
        @Override public Void doVisitInnerNode(String key, InnerNode newNode) {
            // Read same node from old tree.
            InnerNode oldNode = oldInnerNodesStack.peek().traverseChild(key, ConfigurationUtil.innerNodeVisitor());

            // Skip subtree that has not changed.
            if (oldNode == newNode && !singleTreeTraversal)
                return null;

            if (oldNode == null)
                visitAsymmetricInnerNode(newNode, false);
            else {
                oldInnerNodesStack.push(oldNode);

                newNode.traverseChildren(this);

                oldInnerNodesStack.pop();
            }

            return null;
        }

        /** {@inheritDoc} */
        @Override public <N extends InnerNode> Void doVisitNamedListNode(String key, NamedListNode<N> newNode) {
            // Read same named list node from old tree.
            NamedListNode<?> oldNode = oldInnerNodesStack.peek().traverseChild(key, ConfigurationUtil.namedListNodeVisitor());

            // Skip subtree that has not changed.
            if (oldNode == newNode && !singleTreeTraversal)
                return null;

            // Old keys ordering can be ignored if we either create or delete everything.
            Map<String, Integer> oldKeysToOrderIdxMap = singleTreeTraversal ? null
                : keysToOrderIdx(oldNode);

            // New keys ordering can be ignored if we delete everything.
            Map<String, Integer> newKeysToOrderIdxMap = deletion ? null
                : keysToOrderIdx(newNode);

            for (String namedListKey : newNode.namedListKeys()) {
                withTracking(namedListKey, true, false, () -> {
                    InnerNode oldNamedElement = oldNode.get(namedListKey);
                    InnerNode newNamedElement = newNode.get(namedListKey);

                    // Deletion of nonexistent element.
                    if (oldNamedElement == null && newNamedElement == null)
                        return null;

                    // Skip element that has not changed.
                    // Its index can be different though, so we don't "continue" straight away.
                    if (singleTreeTraversal || oldNamedElement != newNamedElement) {
                        if (newNamedElement == null)
                            visitAsymmetricInnerNode(oldNamedElement, true);
                        else if (oldNamedElement == null)
                            visitAsymmetricInnerNode(newNamedElement, false);
                        else {
                            oldInnerNodesStack.push(oldNamedElement);

                            newNamedElement.traverseChildren(this);

                            oldInnerNodesStack.pop();
                        }
                    }

                    Integer newIdx = newKeysToOrderIdxMap == null ? null : newKeysToOrderIdxMap.get(namedListKey);
                    Integer oldIdx = oldKeysToOrderIdxMap == null ? null : oldKeysToOrderIdxMap.get(namedListKey);

                    // We should "persist" changed indexes only.
                    if (newIdx != oldIdx || singleTreeTraversal || newNamedElement == null) {
                        String orderKey = currentKey() + NamedListNode.ORDER_IDX;

                        resMap.put(orderKey, deletion || newNamedElement == null ? null : newIdx);
                    }

                    return null;
                });
            }

            return null;
        }

        /**
         * Here we must list all joined keys belonging to deleted or created element. The only way to do it is to
         * traverse the entire configuration tree unconditionally.
         */
        private void visitAsymmetricInnerNode(InnerNode node, boolean delete) {
            assert !singleTreeTraversal;
            assert node != null;

            oldInnerNodesStack.push(node);
            singleTreeTraversal = true;
            deletion = delete;

            node.traverseChildren(this);

            deletion = false;
            singleTreeTraversal = false;
            oldInnerNodesStack.pop();
        }
    }
}

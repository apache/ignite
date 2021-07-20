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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.ConfigurationProperty;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.notifications.ConfigurationListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.internal.configuration.DynamicConfiguration;
import org.apache.ignite.internal.configuration.DynamicProperty;
import org.apache.ignite.internal.configuration.NamedListConfiguration;
import org.apache.ignite.internal.configuration.notifications.ConfigurationNotificationEventImpl;
import org.apache.ignite.internal.configuration.tree.ConfigurationVisitor;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.apache.ignite.internal.configuration.tree.NamedListNode;

import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.innerNodeVisitor;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.leafNodeVisitor;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.namedListNodeVisitor;

/** */
public class ConfigurationNotificationsUtil {
    /**
     * Recursively notifies all public configuration listeners, accumulating resulting futures in {@code futures} list.
     * @param oldInnerNode Old configuration values root.
     * @param newInnerNode New configuration values root.
     * @param cfgNode Public configuration tree node corresponding to the current inner nodes.
     * @param storageRevision Storage revision.
     * @param futures Write-only list of futures to accumulate results.
     */
    public static void notifyListeners(
        InnerNode oldInnerNode,
        InnerNode newInnerNode,
        DynamicConfiguration<InnerNode, ?> cfgNode,
        long storageRevision,
        List<CompletableFuture<?>> futures
    ) {
        if (oldInnerNode == null || oldInnerNode == newInnerNode)
            return;

        notifyPublicListeners(cfgNode.listeners(), oldInnerNode, newInnerNode, storageRevision, futures);

        oldInnerNode.traverseChildren(new ConfigurationVisitor<Void>() {
            /** {@inheritDoc} */
            @Override public Void visitLeafNode(String key, Serializable oldLeaf) {
                Serializable newLeaf = newInnerNode.traverseChild(key, leafNodeVisitor());

                if (newLeaf != oldLeaf) {
                    var dynProperty = (DynamicProperty<Serializable>)cfgNode.members().get(key);

                    notifyPublicListeners(dynProperty.listeners(), oldLeaf, newLeaf, storageRevision, futures);
                }

                return null;
            }

            /** {@inheritDoc} */
            @Override public Void visitInnerNode(String key, InnerNode oldNode) {
                InnerNode newNode = newInnerNode.traverseChild(key, innerNodeVisitor());

                var dynCfg = (DynamicConfiguration<InnerNode, ?>)cfgNode.members().get(key);

                notifyListeners(oldNode, newNode, dynCfg, storageRevision, futures);

                return null;
            }

            /** {@inheritDoc} */
            @Override public <N extends InnerNode> Void visitNamedListNode(String key, NamedListNode<N> oldNamedList) {
                var newNamedList = (NamedListNode<InnerNode>)newInnerNode.traverseChild(key, namedListNodeVisitor());

                if (newNamedList != oldNamedList) {
                    var namedListCfg = (NamedListConfiguration<?, InnerNode, ?>)cfgNode.members().get(key);

                    notifyPublicListeners(namedListCfg.listeners(), (NamedListView<InnerNode>)oldNamedList, newNamedList, storageRevision, futures);

                    // This is optimization, we could use "NamedListConfiguration#get" directly, but we don't want to.

                    List<String> oldNames = oldNamedList.namedListKeys();
                    List<String> newNames = newNamedList.namedListKeys();

                    Map<String, ConfigurationProperty<?, ?>> namedListCfgMembers = namedListCfg.members();

                    Set<String> created = new HashSet<>(newNames);
                    created.removeAll(oldNames);

                    if (!created.isEmpty()) {
                        List<ConfigurationListener<InnerNode>> list = namedListCfg.extendedListeners()
                            .stream()
                            .map(l -> (ConfigurationListener<InnerNode>)l::onCreate)
                            .collect(Collectors.toList());

                        for (String name : created)
                            notifyPublicListeners(list, null, newNamedList.get(name), storageRevision, futures);
                    }

                    Set<String> deleted = new HashSet<>(oldNames);
                    deleted.removeAll(newNames);

                    if (!deleted.isEmpty()) {
                        List<ConfigurationListener<InnerNode>> list = namedListCfg.extendedListeners()
                            .stream()
                            .map(l -> (ConfigurationListener<InnerNode>)l::onDelete)
                            .collect(Collectors.toList());

                        for (String name : deleted)
                            notifyPublicListeners(list, oldNamedList.get(name), null, storageRevision, futures);
                    }

                    for (String name : newNames) {
                        if (!oldNames.contains(name))
                            continue;

                        notifyPublicListeners(namedListCfg.extendedListeners(), oldNamedList.get(name), newNamedList.get(name), storageRevision, futures);

                        var dynCfg = (DynamicConfiguration<InnerNode, ?>)namedListCfgMembers.get(name);

                        notifyListeners(oldNamedList.get(name), newNamedList.get(name), dynCfg, storageRevision, futures);
                    }
                }

                return null;
            }
        });
    }

    /**
     * Invoke {@link ConfigurationListener#onUpdate(ConfigurationNotificationEvent)} on all passed listeners and put
     * results in {@code futures}. Not recursively.
     * @param listeners List o flisteners.
     * @param oldVal Old configuration value.
     * @param newVal New configuration value.
     * @param storageRevision Storage revision.
     * @param futures Write-only list of futures.
     * @param <V> Type of the node.
     */
    private static <V> void notifyPublicListeners(
        List<? extends ConfigurationListener<V>> listeners,
        V oldVal,
        V newVal,
        long storageRevision,
        List<CompletableFuture<?>> futures
    ) {
        ConfigurationNotificationEvent<V> evt = new ConfigurationNotificationEventImpl<>(
            oldVal,
            newVal,
            storageRevision
        );

        for (ConfigurationListener<V> listener : listeners) {
            try {
                CompletableFuture<?> future = listener.onUpdate(evt);

                if (future != null && (future.isCompletedExceptionally() || future.isCancelled() || !future.isDone()))
                    futures.add(future);
            }
            catch (Throwable t) {
                futures.add(CompletableFuture.failedFuture(t));
            }
        }
    }
}

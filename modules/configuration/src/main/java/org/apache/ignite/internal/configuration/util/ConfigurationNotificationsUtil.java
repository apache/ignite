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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import org.apache.ignite.configuration.ConfigurationProperty;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.notifications.ConfigurationListener;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
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

        notifyPublicListeners(
            cfgNode.listeners(),
            oldInnerNode,
            newInnerNode,
            storageRevision,
            futures,
            ConfigurationListener::onUpdate
        );

        oldInnerNode.traverseChildren(new ConfigurationVisitor<Void>() {
            /** {@inheritDoc} */
            @Override public Void visitLeafNode(String key, Serializable oldLeaf) {
                Serializable newLeaf = newInnerNode.traverseChild(key, leafNodeVisitor());

                if (newLeaf != oldLeaf) {
                    var dynProperty = (DynamicProperty<Serializable>)cfgNode.members().get(key);

                    notifyPublicListeners(
                        dynProperty.listeners(),
                        oldLeaf,
                        newLeaf,
                        storageRevision,
                        futures,
                        ConfigurationListener::onUpdate
                    );
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

                    notifyPublicListeners(
                        namedListCfg.listeners(),
                        (NamedListView<InnerNode>)oldNamedList,
                        newNamedList,
                        storageRevision,
                        futures,
                        ConfigurationListener::onUpdate
                    );

                    // This is optimization, we could use "NamedListConfiguration#get" directly, but we don't want to.
                    List<String> oldNames = oldNamedList.namedListKeys();
                    List<String> newNames = newNamedList.namedListKeys();

                    //TODO https://issues.apache.org/jira/browse/IGNITE-15193
                    Map<String, ConfigurationProperty<?, ?>> namedListCfgMembers = namedListCfg.members();

                    Set<String> created = new HashSet<>(newNames);
                    created.removeAll(oldNames);

                    Set<String> deleted = new HashSet<>(oldNames);
                    deleted.removeAll(newNames);

                    Map<String, String> renamed = new HashMap<>();
                    if (!created.isEmpty() && !deleted.isEmpty()) {
                        Map<String, String> createdIds = new HashMap<>();

                        for (String createdKey : created)
                            createdIds.put(newNamedList.internalId(createdKey), createdKey);

                        // Avoiding ConcurrentModificationException.
                        for (String deletedKey : Set.copyOf(deleted)) {
                            String internalId = oldNamedList.internalId(deletedKey);

                            String maybeRenamedKey = createdIds.get(internalId);

                            if (maybeRenamedKey == null)
                                continue;

                            deleted.remove(deletedKey);
                            created.remove(maybeRenamedKey);
                            renamed.put(deletedKey, maybeRenamedKey);
                        }
                    }

                    if (!created.isEmpty()) {
                        for (String name : created)
                            notifyPublicListeners(
                                namedListCfg.extendedListeners(),
                                null,
                                newNamedList.get(name),
                                storageRevision,
                                futures,
                                ConfigurationNamedListListener::onCreate
                            );
                    }

                    if (!deleted.isEmpty()) {
                        for (String name : deleted) {
                            notifyPublicListeners(
                                namedListCfg.extendedListeners(),
                                oldNamedList.get(name),
                                null,
                                storageRevision,
                                futures,
                                ConfigurationNamedListListener::onDelete
                            );
                        }
                    }

                    if (!renamed.isEmpty()) {
                        for (Map.Entry<String, String> entry : renamed.entrySet()) {
                            notifyPublicListeners(
                                namedListCfg.extendedListeners(),
                                oldNamedList.get(entry.getKey()),
                                newNamedList.get(entry.getValue()),
                                storageRevision,
                                futures,
                                (listener, evt) -> listener.onRename(entry.getKey(), entry.getValue(), evt)
                            );
                        }
                    }

                    for (String name : newNames) {
                        if (!oldNames.contains(name))
                            continue;

                        notifyPublicListeners(
                            namedListCfg.extendedListeners(),
                            oldNamedList.get(name),
                            newNamedList.get(name),
                            storageRevision,
                            futures,
                            ConfigurationListener::onUpdate
                        );

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
     *
     * @param listeners List o flisteners.
     * @param oldVal Old configuration value.
     * @param newVal New configuration value.
     * @param storageRevision Storage revision.
     * @param futures Write-only list of futures.
     * @param updater Update closure to be invoked on the listener instance.
     * @param <V> Type of the node.
     * @param <L> Type of the configuration listener.
     */
    private static <V, L extends ConfigurationListener<V>> void notifyPublicListeners(
        List<? extends L> listeners,
        V oldVal,
        V newVal,
        long storageRevision,
        List<CompletableFuture<?>> futures,
        BiFunction<L, ConfigurationNotificationEvent<V>, CompletableFuture<?>> updater
    ) {
        ConfigurationNotificationEvent<V> evt = new ConfigurationNotificationEventImpl<>(
            oldVal,
            newVal,
            storageRevision
        );

        for (L listener : listeners) {
            try {
                CompletableFuture<?> future = updater.apply(listener, evt);

                if (future != null && (future.isCompletedExceptionally() || future.isCancelled() || !future.isDone()))
                    futures.add(future);
            }
            catch (Throwable t) {
                futures.add(CompletableFuture.failedFuture(t));
            }
        }
    }
}

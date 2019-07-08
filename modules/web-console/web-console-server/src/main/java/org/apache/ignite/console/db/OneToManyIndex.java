/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.console.db;

import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.Function;

import org.apache.ignite.Ignite;
import org.apache.ignite.console.dto.AbstractDto;
import org.apache.ignite.internal.util.typedef.F;

import static java.util.stream.Collectors.toSet;

/**
 * Index for one to many relation.
 */
public class OneToManyIndex<T> extends CacheHolder<T, TreeSet<UUID>> {
    /** Message generator function */
    private final Function<T, String> msgGenerator;

    /**
     * Constructor.
     *
     * @param ignite Ignite.
     * @param idxName Index name.
     */
    public OneToManyIndex(Ignite ignite, String idxName, Function<T, String> msgGenerator) {
        super(ignite, idxName);

        this.msgGenerator = msgGenerator;
    }

    /**
     * @param set Set to check.
     * @return Specified set if it is not {@code null} or new empty {@link TreeSet}.
     */
    private Set<UUID> ensure(Set<UUID> set) {
        return (set == null) ? new TreeSet<>() : set;
    }

    /**
     * @param parentId Parent ID.
     * @return Set of children IDs.
     */
    public Set<UUID> load(T parentId) {
        return ensure(cache().get(parentId));
    }

    /**
     * Put child ID to index.
     *
     * @param parentId Parent ID.
     * @param child Child ID to add.
     */
    public void add(T parentId, UUID child) {
        Set<UUID> childrenIds = load(parentId);

        childrenIds.add(child);

        cache.put(parentId, childrenIds);
    }

    /**
     * Put children IDs to index.
     *
     * @param parent Parent ID.
     * @param childrenToAdd Children IDs to add.
     */
    public void addAll(T parent, Set<UUID> childrenToAdd) {
        Set<UUID> children = load(parent);

        children.addAll(childrenToAdd);

        cache.put(parent, children);
    }

    /**
     * Remove child ID from index.
     *
     * @param parent Parent ID.
     * @param child Child ID to remove.
     */
    public void remove(T parent, UUID child) {
        Set<UUID> children = load(parent);

        children.remove(child);

        cache.put(parent, children);
    }

    /**
     * Remove children IDs from index.
     *
     * @param parent Parent ID.
     * @param childrenToRmv Children IDs to remove.
     */
    public void removeAll(T parent, Set<UUID> childrenToRmv) {
        Set<UUID> children = load(parent);

        children.removeAll(childrenToRmv);

        cache.put(parent, children);
    }

    /**
     * Delete entry from index.
     *
     * @param parent Parent ID to delete.
     * @return Children IDs associated with parent ID.
     */
    public Set<UUID> delete(T parent) {
        return ensure(cache().getAndRemove(parent));
    }

    /**
     * Validate that parent has specified child.
     *
     * @param parent Parent key.
     * @param child Child key.
     */
    public void validate(T parent, UUID child) {
        Set<UUID> children = load(parent);

        if (!children.contains(child))
            throw new IllegalStateException(message(parent));
    }

    /**
     * Validate that child belong to the parent or child does not exist yet.
     *
     * @param parent Parent key.
     * @param child Child key.
     * @param tbl Table.
     */
    public void validateBeforeSave(T parent, UUID child, Table<? extends AbstractDto> tbl) {
        if (!tbl.contains(child))
            return;

        validate(parent, child);
    }

    /**
     * Validate that parent has all specified children.
     *
     * @param parent Parent key.
     * @param children Children keys.
     */
    public void validateAll(T parent, Collection<UUID> children) {
        Set<UUID> allChildren = load(parent);

        if (!allChildren.containsAll(children))
            throw new IllegalStateException(message(parent));
    }

    /**
     * Validate that children belong to the parent or children does not exist yet.
     *
     * @param parent Parent key.
     * @param children Children keys.
     */
    public void validateBeforeSave(T parent, Collection<UUID> children, Table<? extends AbstractDto> tbl) {
        Set<UUID> existing = children.stream().filter(tbl::contains).collect(toSet());

        if (F.isEmpty(existing))
            return;

        validateAll(parent, existing);
    }

    /**
     * @param val Value.
     *
     * @return Message.
     */
    public String message(T val) {
        return msgGenerator.apply(val);
    }
}



package org.apache.ignite.console.db;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.Function;
import org.apache.ignite.Ignite;
import org.apache.ignite.console.dto.AbstractDto;
import org.apache.ignite.console.messages.WebConsoleMessageSource;
import org.apache.ignite.console.messages.WebConsoleMessageSourceAccessor;
import org.apache.ignite.internal.util.typedef.F;

import static java.util.stream.Collectors.toSet;

/**
 * Index for one to many relation.
 */
public class OneToManyIndex<K, V> extends CacheHolder<K, Set<V>> {
    /** Messages accessor. */
    private static WebConsoleMessageSourceAccessor messages = WebConsoleMessageSource.getAccessor();

    /** Message generator function */
    private final Function<K, String> msgGenerator;

    /**
     * Constructor.
     *
     * @param ignite Ignite.
     * @param cacheName Cache name.
     */
    public OneToManyIndex(Ignite ignite, String cacheName) {
        this(ignite, cacheName, -1);
    }

    /**
     * Constructor.
     *
     * @param ignite Ignite.
     * @param cacheName Cache name.
     * @param expirationTimeout Cache expiration timeout.
     */
    public OneToManyIndex(
        Ignite ignite,
        String cacheName,
        long expirationTimeout
    ) {
        super(ignite, Table.INDEX_DB_PREFIX + cacheName, expirationTimeout);

        this.msgGenerator = (key) -> messages.getMessage("err.data-access-violation");
    }

    /**
     * @param set Set to check.
     * @return Specified set if it is not {@code null} or new empty {@link TreeSet}.
     */
    private Set<V> ensure(Set<V> set) {
        return (set == null) ? new HashSet<>() : set;
    }

    /**
     * @param parentId Parent ID.
     * @return Set of children IDs.
     */
    @Override public Set<V> get(K parentId) {
        return ensure(super.get(parentId));
    }

    /**
     * @param parentIds Parent IDs.
     * @return Set of children IDs.
     */
    public Set<V> getAll(Set<K> parentIds) {
        return parentIds
            .stream()
            .flatMap(accId -> get(accId).stream())
            .collect(toSet());
    }

    /**
     * Put child ID to index.
     *
     * @param parentId Parent ID.
     * @param child Child ID to add.
     */
    public void add(K parentId, V child) {
        Set<V> childrenIds = get(parentId);

        if(childrenIds.add(child))
        	cache().put(parentId, childrenIds);
    }

    /**
     * Put children IDs to index.
     *
     * @param parent Parent ID.
     * @param childrenToAdd Children IDs to add.
     */
    public void addAll(K parent, Set<V> childrenToAdd) {
        Set<V> children = get(parent);

        if(children.addAll(childrenToAdd))
        	cache().put(parent, children);
    }

    /**
     * Remove child ID from index.
     *
     * @param parent Parent ID.
     * @param child Child ID to remove.
     */
    public void remove(K parent, V child) {
        Set<V> children = get(parent);

        if(children.remove(child))
        	cache().put(parent, children);
    }

    /**
     * Remove children IDs from index.
     *
     * @param parent Parent ID.
     * @param childrenToRmv Children IDs to remove.
     */
    public void removeAll(K parent, Set<V> childrenToRmv) {
        Set<V> children = get(parent);

        children.removeAll(childrenToRmv);

        cache().put(parent, children);
    }

    /**
     * Delete entry from index.
     *
     * @param parent Parent ID to delete.
     * @return Children IDs associated with parent ID.
     */
    public Set<V> delete(K parent) {
        return ensure(cache().getAndRemove(parent));
    }

    /**
     * Validate that parent has specified child.
     *
     * @param parent Parent key.
     * @param child Child key.
     */
    @SuppressWarnings("SuspiciousMethodCalls")
    public void validate(K parent, Object child) {
        Set<V> children = get(parent);

        if (children != null && !children.contains(child))
            throw new IllegalStateException(message(parent));
    }

    /**
     * Validate that child belong to the parent or child does not exist yet.
     *
     * @param parent Parent key.
     * @param child Child key.
     * @param tbl Table.
     */
    public <U extends UUID> void validateBeforeSave(K parent, U child, Table<? extends AbstractDto> tbl) {
        if (!tbl.containsKey(child))
            return;

        validate(parent, child);
    }

    /**
     * Validate that parent has all specified children.
     *
     * @param parent Parent key.
     * @param children Children keys.
     */
    public void validateAll(K parent, Collection<? extends UUID> children) {
        Set<V> allChildren = get(parent);

        if (allChildren != null && !allChildren.containsAll(children))
            throw new IllegalStateException(message(parent));
    }

    /**
     * Validate that children belong to the parent or children does not exist yet.
     *
     * @param parent Parent key.
     * @param children Children keys.
     */
    public void validateBeforeSave(K parent, Collection<? extends UUID> children, Table<? extends AbstractDto> tbl) {
        Set<? extends UUID> existing = children.stream().filter(tbl::containsKey).collect(toSet());

        if (F.isEmpty(existing))
            return;

        validateAll(parent, existing);
    }

    /**
     * @param parent Parent key.
     *
     * @return Message.
     */
    public String message(K parent) {
        return msgGenerator.apply(parent);
    }
}

// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.query;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public interface GridCacheQueryClassResolver {
    public String resolveTypeName(Object o);

    public Class<?> resolveFieldType(String field, Object o);

    public Object valueOf(String field, Object o);
}

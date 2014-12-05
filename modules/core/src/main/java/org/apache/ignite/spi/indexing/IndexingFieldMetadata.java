/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi.indexing;

import java.io.*;

/**
 * Query field descriptor. This descriptor is used to provide metadata
 * about fields returned in query result.
 */
public interface IndexingFieldMetadata extends Externalizable {
    /**
     * Gets schema name.
     *
     * @return Schema name.
     */
    public String schemaName();

    /**
     * Gets name of type to which this field belongs.
     *
     * @return Gets type name.
     */
    public String typeName();

    /**
     * Gets field name.
     *
     * @return Field name.
     */
    public String fieldName();

    /**
     * Gets field type name.
     *
     * @return Field type name.
     */
    public String fieldTypeName();
}

/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.marshaller.optimized;

/**
 * Field type used to calculate {@code Unsafe} offsets into objects.
 */
enum IgniteOptimizedFieldType {
    /** */
    BYTE,

    /** */
    SHORT,

    /** */
    INT,

    /** */
    LONG,

    /** */
    FLOAT,

    /** */
    DOUBLE,

    /** */
    CHAR,

    /** */
    BOOLEAN,

    /** */
    OTHER
}

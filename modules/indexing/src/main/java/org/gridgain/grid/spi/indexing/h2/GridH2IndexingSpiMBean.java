/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.indexing.h2;

import org.gridgain.grid.spi.*;
import org.gridgain.grid.util.mbean.*;
import org.h2.api.*;
import org.jetbrains.annotations.*;

/**
 * Management bean for {@link GridH2IndexingSpi}.
 */
@GridMBeanDescription("MBean that provides access to H2 indexing SPI configuration.")
public interface GridH2IndexingSpiMBean extends GridSpiManagementMBean {
    /**
     * Gets if SPI will index primitive key-value pairs by key.
     *
     * @return Flag value.
     */
    @GridMBeanDescription("Index primitive key-value pairs by key by default in all schemas.")
    public boolean isDefaultIndexPrimitiveKey();

    /**
     * Gets if SPI will index primitive key-value pairs by value. Makes sense only if
     * {@link GridH2IndexingSpi#setDefaultIndexPrimitiveKey(boolean)} set to true.
     *
     * @return Flag value.
     */
    @GridMBeanDescription("Index primitive key-value pairs by value by default in all schemas.")
    public boolean isDefaultIndexPrimitiveValue();

    /**
     * If false, SPI will store all keys in BINARY form to make it possible to store
     * the same value type with different key types. If true, key type will be converted
     * to respective SQL type if it is possible.
     *
     * @return Flag value.
     */
    @GridMBeanDescription("If false, SPI will store all keys in BINARY form, otherwise it will try to convert key " +
        "type to respective SQL type.")
    public boolean isDefaultIndexFixedTyping();

    /**
     * Gets maximum amount of memory available to off-heap storage. Possible values are
     * <ul>
     * <li>{@code -1} - Means that off-heap storage is disabled.</li>
     * <li>
     *     {@code 0} - GridGain will not limit off-heap storage (it's up to user to properly
     *     add and remove entries from cache to ensure that off-heap storage does not grow
     *     indefinitely.
     * </li>
     * <li>Any positive value specifies the limit of off-heap storage in bytes.</li>
     * </ul>
     * Default value is {@code -1}, which means that off-heap storage is disabled by default.
     * <p>
     * Use off-heap storage to load gigabytes of data in memory without slowing down
     * Garbage Collection. Essentially in this case you should allocate very small amount
     * of memory to JVM and GridGain will cache most of the data in off-heap space
     * without affecting JVM performance at all.
     *
     * @return Maximum memory in bytes available to off-heap memory space.
     */
    @GridMBeanDescription("Maximum off-heap memory for indexes.")
    public long getMaxOffHeapMemory();

    /**
     * Gets index write lock wait time in milliseconds. This parameter can affect query performance under high
     * thread contention. Default value is {@code 100}.
     *
     * @return Index write lock wait time in milliseconds.
     */
    @GridMBeanDescription("Index write lock wait time in milliseconds.")
    public long getIndexWriteLockWaitTime();

    /**
     * Gets maximum allowed off-heap rows cache size in items.
     *
     * @return Maximum allowed off-heap rows cache size.
     */
    @GridMBeanDescription("Maximum allowed off-heap rows cache size.")
    public int getMaxOffheapRowsCacheSize();

    /**
     * Gets current off-heap rows cache size in items.
     *
     * @return Current off-heap rows cache size.
     */
    @GridMBeanDescription("Current off-heap rows cache size.")
    public int getOffheapRowsCacheSize();

    /**
     * Gets currently allocated offheap memory.
     *
     * @return Allocated memory in bytes.
     */
    @GridMBeanDescription("Allocated off-heap memory for indexes.")
    public long getAllocatedOffHeapMemory();

    /**
     * Gets all registered in this SPI spaces comma separated.
     *
     * @return Collection of space names.
     */
    @GridMBeanDescription("All registered in this SPI spaces.")
    public String getSpaceNames();

    /**
     * Gets the optional search path consisting of space names to search SQL schema objects. Useful for cross cache
     * queries to avoid writing fully qualified table names.
     *
     * @return Array of space names.
     */
    @Nullable public String[] getSearchPath();

    /**
     * The flag indicating that {@link JavaObjectSerializer} for H2 database will be set to optimized version.
     *
     * @return Flag value.
     */
    public boolean getUseOptimizedSerializer();

    /**
     * Gets script path to be ran against H2 database after opening.
     *
     * @return Script path.
     */
    @Nullable public String getInitialScriptPath();

    /**
     * Get query execution time interpreted by SPI as long query for additional handling (e.g. print warning).
     *
     * @return Long query execution time.
     * @see #isLongQueryExplain()
     */
    @GridMBeanDescription("Query execution time interpreted by SPI as long query.")
    public long getLongQueryExecutionTimeout();

    /**
     * If {@code true}, SPI prints SQL execution plan for long queries (explain SQL query).
     *
     * @return Flag marking SPI should print SQL execution plan for long queries (explain SQL query).
     * @see #getLongQueryExecutionTimeout()
     */
    @GridMBeanDescription("If true, SPI will print SQL execution plan for long queries (explain SQL query).")
    public boolean isLongQueryExplain();

    /**
     * Defines whether indexing SPI will index by key entries where key and value are primitive types in given space.
     *
     * @param spaceName Space name.
     * @return Flag value.
     */
    @GridMBeanDescription("Index by key entries where key and value are primitive types in given space.")
    @GridMBeanParametersNames("spaceName")
    @GridMBeanParametersDescriptions("Space name.")
    public boolean isIndexPrimitiveKey(@Nullable String spaceName);

    /**
     * Defines whether indexing SPI will index by value entries where key and value are primitive types in given space.
     *
     * @param spaceName Space name.
     * @return Flag value.
     */
    @GridMBeanDescription("Index by value entries where key and value are primitive types in given space.")
    @GridMBeanParametersNames("spaceName")
    @GridMBeanParametersDescriptions("Space name.")
    public boolean isIndexPrimitiveValue(@Nullable String spaceName);

    /**
     * If {@code false}, SPI will store all keys in BINARY form to make it possible to store
     * the same value type with different key types. If {@code true}, key type will be converted
     * to respective SQL type if it is possible.
     * <p>
     * Setting this value to {@code false} also means that {@code '_key'} column cannot be indexed and
     * cannot participate in query where clauses. The behavior of using '_key' column in where
     * clauses with this flag set to {@code false} is undefined.
     *
     * @param spaceName Space name.
     * @return Flag value.
     */
    @GridMBeanDescription("If false, SPI will store all keys in BINARY form, otherwise it will try to convert key " +
        "type to respective SQL type.")
    @GridMBeanParametersNames("spaceName")
    @GridMBeanParametersDescriptions("Space name.")
    public boolean isIndexFixedTyping(@Nullable String spaceName);

    /**
     * If {@code true}, then table name and all column names in 'create table' SQL
     * generated by SPI are escaped with double quotes. This flag should be set if table name of
     * column name is H2 reserved word or is not valid H2 identifier (e.g. contains space or hyphen).
     * <p>
     * Note if this flag is set then table and column name in SQL queries also must be escaped with double quotes.
     *
     * @return Flag value.
     */
    @GridMBeanDescription("If true, then table name and all column names in 'create table' SQL generated by " +
        "SPI are escaped with double quotes.")
    public boolean isDefaultEscapeAll();

    /**
     * If {@code true}, then table name and all column names in 'create table' SQL
     * generated by SPI are escaped with double quotes. This flag should be set if table name of
     * column name is H2 reserved word or is not valid H2 identifier (e.g. contains space or hyphen).
     * <p>
     * Note if this flag is set then table and column name in SQL queries also must be escaped with double quotes.
     *
     * @param spaceName Space name.
     * @return Flag value.
     */
    @GridMBeanDescription("If true, then table name and all column names in 'create table' SQL generated by " +
        "SPI are escaped with double quotes.")
    @GridMBeanParametersNames("spaceName")
    @GridMBeanParametersDescriptions("Space name.")
    public boolean isEscapeAll(@Nullable String spaceName);
}

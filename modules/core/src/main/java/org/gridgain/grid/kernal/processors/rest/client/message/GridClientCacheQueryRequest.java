/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest.client.message;

import org.gridgain.portable.*;
import org.jetbrains.annotations.*;

/**
 * Cache query request.
 */
public class GridClientCacheQueryRequest extends GridClientAbstractMessage {
    /**
     * Available query operations.
     */
    @SuppressWarnings("PublicInnerClass")
    public enum GridQueryOperation {
        /** First time query execution. Will assign query ID for executed query. */
        EXECUTE,

        /** Fetch next data page. */
        FETCH,

        /** Rebuild one or all indexes. */
        REBUILD_INDEXES;

        /** Enumerated values. */
        private static final GridQueryOperation[] VALS = values();

        /**
         * Efficiently gets enumerated value from its ordinal.
         *
         * @param ord Ordinal value.
         * @return Enumerated value or {@code null} if ordinal out of range.
         */
        @Nullable public static GridQueryOperation fromOrdinal(int ord) {
            return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
        }
    }

    /**
     * Query types.
     */
    @SuppressWarnings("PublicInnerClass")
    public enum GridQueryType {
        /** SQL query. */
        SQL,

        /** SQL fields query. */
        SQL_FIELDS,

        /** Full text query. */
        FULL_TEXT,

        /** Scan query. */
        SCAN;

        /** Enumerated values. */
        private static final GridQueryType[] VALS = values();

        /**
         * Efficiently gets enumerated value from its ordinal.
         *
         * @param ord Ordinal value.
         * @return Enumerated value or {@code null} if ordinal out of range.
         */
        @Nullable public static GridQueryType fromOrdinal(int ord) {
            return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
        }
    }

    /** Query ID linked to destination node ID. */
    private long qryId;

    /** Query operation. */
    private GridQueryOperation op;

    /** Cache name. */
    private String cacheName;

    /** Query type. */
    private GridQueryType type;

    /** Query clause. */
    private String clause;

    /** Page size. */
    private int pageSize;

    /** Timeout. */
    private long timeout;

    /** Include backups flag. */
    private boolean includeBackups;

    /** Enable dedup flag. */
    private boolean enableDedup;

    /** Class name. */
    private String clsName;

    /** Remote reducer class name. */
    private String rmtReducerClsName;

    /** Remote transformer class name. */
    private String rmtTransformerClsName;

    /** Query arguments. */
    private Object[] qryArgs;

    /**
     * @return Query ID.
     */
    public long queryId() {
        return qryId;
    }

    /**
     * @param qryId Query ID.
     */
    public void queryId(long qryId) {
        this.qryId = qryId;
    }

    /**
     * @return Operation.
     */
    public GridQueryOperation operation() {
        return op;
    }

    /**
     * @param op Operation.
     */
    public void operation(GridQueryOperation op) {
        this.op = op;
    }

    /**
     * @return Cache name.
     */
    public String cacheName() {
        return cacheName;
    }

    /**
     * @param cacheName Cache name.
     */
    public void cacheName(String cacheName) {
        this.cacheName = cacheName;
    }

    /**
     * @return Query type.
     */
    public GridQueryType type() {
        return type;
    }

    /**
     * @param type Query type.
     */
    public void type(GridQueryType type) {
        this.type = type;
    }

    /**
     * @return Query clause.
     */
    public String clause() {
        return clause;
    }

    /**
     * @param clause Query clause.
     */
    public void clause(String clause) {
        this.clause = clause;
    }

    /**
     * @return Page size.
     */
    public int pageSize() {
        return pageSize;
    }

    /**
     * @param pageSize Page size.
     */
    public void pageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    /**
     * @return Query timeout.
     */
    public long timeout() {
        return timeout;
    }

    /**
     * @param timeout Query timeout.
     */
    public void timeout(long timeout) {
        this.timeout = timeout;
    }

    /**
     * @return Include backups flag.
     */
    public boolean includeBackups() {
        return includeBackups;
    }

    /**
     * @param includeBackups Include backups flag.
     */
    public void includeBackups(boolean includeBackups) {
        this.includeBackups = includeBackups;
    }

    /**
     * @return Enable de-duplication flag.
     */
    public boolean enableDedup() {
        return enableDedup;
    }

    /**
     * @param enableDedup Enable de-duplication flag.
     */
    public void enableDedup(boolean enableDedup) {
        this.enableDedup = enableDedup;
    }

    /**
     * @return Class name.
     */
    public String className() {
        return clsName;
    }

    /**
     * @param clsName Class name.
     */
    public void className(String clsName) {
        this.clsName = clsName;
    }

    /**
     * @return Remote reducer class name.
     */
    public String remoteReducerClassName() {
        return rmtReducerClsName;
    }

    /**
     * @param rmtReducerClsName Remote reducer class name.
     */
    public void remoteReducerClassName(String rmtReducerClsName) {
        this.rmtReducerClsName = rmtReducerClsName;
    }

    /**
     * @return Remote transformer class name.
     */
    public String remoteTransformerClassName() {
        return rmtTransformerClsName;
    }

    /**
     * @param rmtTransformerClsName Remote transformer class name.
     */
    public void remoteTransformerClassName(String rmtTransformerClsName) {
        this.rmtTransformerClsName = rmtTransformerClsName;
    }

    /**
     * @return Query arguments.
     */
    public Object[] queryArguments() {
        return qryArgs;
    }

    /**
     * @param qryArgs Query arguments.
     */
    public void queryArguments(Object[] qryArgs) {
        this.qryArgs = qryArgs;
    }

    /** {@inheritDoc} */
    @Override public void readPortable(GridPortableReader reader) throws GridPortableException {
        super.readPortable(reader);

        GridPortableRawReader rawReader = reader.rawReader();

        qryId = rawReader.readLong();
        op = GridQueryOperation.fromOrdinal(rawReader.readInt());
        type = GridQueryType.fromOrdinal(rawReader.readInt());
        cacheName = rawReader.readString();
        clause = rawReader.readString();
        pageSize = rawReader.readInt();
        timeout = rawReader.readLong();
        includeBackups = rawReader.readBoolean();
        enableDedup = rawReader.readBoolean();
        clsName = rawReader.readString();
        rmtReducerClsName = rawReader.readString();
        rmtTransformerClsName = rawReader.readString();
        qryArgs = rawReader.readObjectArray();
    }

    /** {@inheritDoc} */
    @Override public void writePortable(GridPortableWriter writer) throws GridPortableException {
        super.writePortable(writer);

        GridPortableRawWriter rawWriter = writer.rawWriter();

        rawWriter.writeLong(qryId);
        rawWriter.writeInt(op.ordinal());
        rawWriter.writeInt(type.ordinal());
        rawWriter.writeString(cacheName);
        rawWriter.writeString(clause);
        rawWriter.writeInt(pageSize);
        rawWriter.writeLong(timeout);
        rawWriter.writeBoolean(includeBackups);
        rawWriter.writeBoolean(enableDedup);
        rawWriter.writeString(clsName);
        rawWriter.writeString(rmtReducerClsName);
        rawWriter.writeString(rmtTransformerClsName);
        rawWriter.writeObjectArray(qryArgs);
    }
}

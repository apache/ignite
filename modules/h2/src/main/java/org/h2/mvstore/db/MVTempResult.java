/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.db;

import java.io.IOException;
import java.lang.ref.Reference;
import java.util.Collection;

import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.expression.Expression;
import org.h2.message.DbException;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.MVStore.Builder;
import org.h2.result.ResultExternal;
import org.h2.result.SortOrder;
import org.h2.store.fs.FileUtils;
import org.h2.util.TempFileDeleter;
import org.h2.value.TypeInfo;
import org.h2.value.Value;

/**
 * Temporary result.
 *
 * <p>
 * A separate MVStore in a temporary file is used for each result. The file is
 * removed when this result and all its copies are closed.
 * {@link TempFileDeleter} is also used to delete this file if results are not
 * closed properly.
 * </p>
 */
public abstract class MVTempResult implements ResultExternal {

    private static final class CloseImpl implements AutoCloseable {
        /**
         * MVStore.
         */
        private final MVStore store;

        /**
         * File name.
         */
        private final String fileName;

        CloseImpl(MVStore store, String fileName) {
            this.store = store;
            this.fileName = fileName;
        }

        @Override
        public void close() throws Exception {
            store.closeImmediately();
            FileUtils.tryDelete(fileName);
        }

    }

    /**
     * Creates MVStore-based temporary result.
     *
     * @param database
     *            database
     * @param expressions
     *            expressions
     * @param distinct
     *            is output distinct
     * @param distinctIndexes
     *            indexes of distinct columns for DISTINCT ON results
     * @param visibleColumnCount
     *            count of visible columns
     * @param sort
     *            sort order, or {@code null}
     * @return temporary result
     */
    public static ResultExternal of(Database database, Expression[] expressions, boolean distinct,
            int[] distinctIndexes, int visibleColumnCount, SortOrder sort) {
        return distinct || distinctIndexes != null || sort != null
                ? new MVSortedTempResult(database, expressions, distinct, distinctIndexes, visibleColumnCount, sort)
                : new MVPlainTempResult(database, expressions, visibleColumnCount);
    }

    /**
     * MVStore.
     */
    final MVStore store;

    /**
     * Column expressions.
     */
    final Expression[] expressions;

    /**
     * Count of visible columns.
     */
    final int visibleColumnCount;

    final boolean hasEnum;

    /**
     * Count of rows. Used only in a root results, copies always have 0 value.
     */
    int rowCount;

    /**
     * Parent store for copies. If {@code null} this result is a root result.
     */
    final MVTempResult parent;

    /**
     * Count of child results.
     */
    int childCount;

    /**
     * Whether this result is closed.
     */
    boolean closed;

    /**
     * Temporary file deleter.
     */
    private final TempFileDeleter tempFileDeleter;

    /**
     * Closeable to close the storage.
     */
    private final CloseImpl closeable;

    /**
     * Reference to the record in the temporary file deleter.
     */
    private final Reference<?> fileRef;

    /**
     * Creates a shallow copy of the result.
     *
     * @param parent
     *                   parent result
     */
    MVTempResult(MVTempResult parent) {
        this.parent = parent;
        this.store = parent.store;
        this.expressions = parent.expressions;
        this.visibleColumnCount = parent.visibleColumnCount;
        this.hasEnum = parent.hasEnum;
        this.tempFileDeleter = null;
        this.closeable = null;
        this.fileRef = null;
    }

    /**
     * Creates a new temporary result.
     *
     * @param database
     *            database
     * @param expressions
     *            column expressions
     * @param visibleColumnCount
     *            count of visible columns
     */
    MVTempResult(Database database, Expression[] expressions, int visibleColumnCount) {
        try {
            String fileName = FileUtils.createTempFile("h2tmp", Constants.SUFFIX_TEMP_FILE, true);
            Builder builder = new MVStore.Builder().fileName(fileName).cacheSize(0).autoCommitDisabled();
            byte[] key = database.getFileEncryptionKey();
            if (key != null) {
                builder.encryptionKey(MVTableEngine.decodePassword(key));
            }
            store = builder.open();
            this.expressions = expressions;
            this.visibleColumnCount = visibleColumnCount;
            boolean hasEnum = false;
            for (Expression e : expressions) {
                if (e.getType().getValueType() == Value.ENUM) {
                    hasEnum = true;
                    break;
                }
            }
            this.hasEnum = hasEnum;
            tempFileDeleter = database.getTempFileDeleter();
            closeable = new CloseImpl(store, fileName);
            fileRef = tempFileDeleter.addFile(closeable, this);
        } catch (IOException e) {
            throw DbException.convert(e);
        }
        parent = null;
    }

    @Override
    public int addRows(Collection<Value[]> rows) {
        for (Value[] row : rows) {
            addRow(row);
        }
        return rowCount;
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (parent != null) {
            parent.closeChild();
        } else {
            if (childCount == 0) {
                delete();
            }
        }
    }

    private synchronized void closeChild() {
        if (--childCount == 0 && closed) {
            delete();
        }
    }

    private void delete() {
        tempFileDeleter.deleteFile(fileRef, closeable);
    }

    /**
     * If any value in the rows is a ValueEnum, apply custom type conversion.
     *
     * @param row the array of values (modified in-place if needed)
     */
    final void fixEnum(Value[] row) {
        for (int i = 0, l = expressions.length; i < l; i++) {
            TypeInfo type = expressions[i].getType();
            if (type.getValueType() == Value.ENUM) {
                row[i] = type.getExtTypeInfo().cast(row[i]);
            }
        }
    }

}

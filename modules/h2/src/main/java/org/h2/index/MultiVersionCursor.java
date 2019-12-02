/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import org.h2.engine.Session;
import org.h2.engine.SysProperties;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;

/**
 * The cursor implementation for the multi-version index.
 */
public class MultiVersionCursor implements Cursor {

    private final MultiVersionIndex index;
    private final Session session;
    private final Cursor baseCursor, deltaCursor;
    private final Object sync;
    private SearchRow baseRow;
    private Row deltaRow;
    private boolean onBase;
    private boolean end;
    private boolean needNewDelta, needNewBase;
    private boolean reverse;

    MultiVersionCursor(Session session, MultiVersionIndex index, Cursor base,
            Cursor delta, Object sync) {
        this.session = session;
        this.index = index;
        this.baseCursor = base;
        this.deltaCursor = delta;
        this.sync = sync;
        needNewDelta = true;
        needNewBase = true;
    }

    /**
     * Load the current row.
     */
    void loadCurrent() {
        synchronized (sync) {
            baseRow = baseCursor.getSearchRow();
            deltaRow = deltaCursor.get();
            needNewDelta = false;
            needNewBase = false;
        }
    }

    private void loadNext(boolean base) {
        synchronized (sync) {
            if (base) {
                if (step(baseCursor)) {
                    baseRow = baseCursor.getSearchRow();
                } else {
                    baseRow = null;
                }
            } else {
                if (step(deltaCursor)) {
                    deltaRow = deltaCursor.get();
                } else {
                    deltaRow = null;
                }
            }
        }
    }

    private boolean step(Cursor cursor) {
        return reverse ? cursor.previous() : cursor.next();
    }

    @Override
    public Row get() {
        synchronized (sync) {
            if (end) {
                return null;
            }
            return onBase ? baseCursor.get() : deltaCursor.get();
        }
    }

    @Override
    public SearchRow getSearchRow() {
        synchronized (sync) {
            if (end) {
                return null;
            }
            return onBase ? baseCursor.getSearchRow() : deltaCursor.getSearchRow();
        }
    }

    @Override
    public boolean next() {
        synchronized (sync) {
            if (SysProperties.CHECK && end) {
                DbException.throwInternalError();
            }
            while (true) {
                if (needNewDelta) {
                    loadNext(false);
                    needNewDelta = false;
                }
                if (needNewBase) {
                    loadNext(true);
                    needNewBase = false;
                }
                if (deltaRow == null) {
                    if (baseRow == null) {
                        end = true;
                        return false;
                    }
                    onBase = true;
                    needNewBase = true;
                    return true;
                }
                int sessionId = deltaRow.getSessionId();
                boolean isThisSession = sessionId == session.getId();
                boolean isDeleted = deltaRow.isDeleted();
                if (isThisSession && isDeleted) {
                    needNewDelta = true;
                    continue;
                }
                if (baseRow == null) {
                    if (isDeleted) {
                        if (isThisSession) {
                            end = true;
                            return false;
                        }
                        // the row was deleted by another session: return it
                        onBase = false;
                        needNewDelta = true;
                        return true;
                    }
                    DbException.throwInternalError();
                }
                int compare = index.compareRows(deltaRow, baseRow);
                if (compare == 0) {
                    // can't use compareKeys because the
                    // version would be compared as well
                    long k1 = deltaRow.getKey();
                    long k2 = baseRow.getKey();
                    compare = Long.compare(k1, k2);
                }
                if (compare == 0) {
                    if (isDeleted) {
                        if (isThisSession) {
                            DbException.throwInternalError();
                        }
                        // another session updated the row
                    } else {
                        if (isThisSession) {
                            onBase = false;
                            needNewBase = true;
                            needNewDelta = true;
                            return true;
                        }
                        // another session inserted the row: ignore
                        needNewBase = true;
                        needNewDelta = true;
                        continue;
                    }
                }
                if (compare > 0) {
                    onBase = true;
                    needNewBase = true;
                    return true;
                }
                onBase = false;
                needNewDelta = true;
                return true;
            }
        }
    }

    @Override
    public boolean previous() {
        reverse = true;
        try {
            return next();
        } finally {
            reverse = false;
        }
    }

}

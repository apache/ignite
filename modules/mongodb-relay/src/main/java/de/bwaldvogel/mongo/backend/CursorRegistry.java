package de.bwaldvogel.mongo.backend;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import de.bwaldvogel.mongo.exception.CursorNotFoundException;

public class CursorRegistry {

    private final ConcurrentMap<Long, Cursor> cursors = new ConcurrentHashMap<>();
    private final AtomicLong cursorIdCounter = new AtomicLong();

    public long generateCursorId() {
        return cursorIdCounter.incrementAndGet();
    }

    public Cursor getCursor(long cursorId) {
        Cursor cursor = cursors.get(cursorId);
        if (cursor == null) {
            throw new CursorNotFoundException(cursorId);
        }
        return cursor;
    }

    public boolean remove(Cursor cursor) {
        return remove(cursor.getId());
    }

    public boolean remove(long cursorId) {
        return cursors.remove(cursorId) != null;
    }

    public void add(Cursor cursor) {
        Cursor previousValue = cursors.put(cursor.getId(), cursor);
        Assert.isNull(previousValue);
    }

    public int size() {
        return cursors.size();
    }

}

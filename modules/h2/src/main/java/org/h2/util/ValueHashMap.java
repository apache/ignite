/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.util.ArrayList;
import org.h2.message.DbException;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * This hash map supports keys of type Value.
 *
 * @param <V> the value type
 */
public class ValueHashMap<V> extends HashBase {

    private Value[] keys;
    private V[] values;

    /**
     * Create a new value hash map.
     *
     * @return the object
     */
    public static <T> ValueHashMap<T> newInstance() {
        return new ValueHashMap<>();
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void reset(int newLevel) {
        super.reset(newLevel);
        keys = new Value[len];
        values = (V[]) new Object[len];
    }

    @Override
    protected void rehash(int newLevel) {
        Value[] oldKeys = keys;
        V[] oldValues = values;
        reset(newLevel);
        int len = oldKeys.length;
        for (int i = 0; i < len; i++) {
            Value k = oldKeys[i];
            if (k != null && k != ValueNull.DELETED) {
                // skip the checkSizePut so we don't end up
                // accidentally recursing
                internalPut(k, oldValues[i]);
            }
        }
    }

    private int getIndex(Value key) {
        return key.hashCode() & mask;
    }

    /**
     * Add or update a key value pair.
     *
     * @param key the key
     * @param value the new value
     */
    public void put(Value key, V value) {
        checkSizePut();
        internalPut(key, value);
    }

    private void internalPut(Value key, V value) {
        int index = getIndex(key);
        int plus = 1;
        int deleted = -1;
        do {
            Value k = keys[index];
            if (k == null) {
                // found an empty record
                if (deleted >= 0) {
                    index = deleted;
                    deletedCount--;
                }
                size++;
                keys[index] = key;
                values[index] = value;
                return;
            } else if (k == ValueNull.DELETED) {
                // found a deleted record
                if (deleted < 0) {
                    deleted = index;
                }
            } else if (k.equals(key)) {
                // update existing
                values[index] = value;
                return;
            }
            index = (index + plus++) & mask;
        } while (plus <= len);
        // no space
        DbException.throwInternalError("hashmap is full");
    }

    /**
     * Remove a key value pair.
     *
     * @param key the key
     */
    public void remove(Value key) {
        checkSizeRemove();
        int index = getIndex(key);
        int plus = 1;
        do {
            Value k = keys[index];
            if (k == null) {
                // found an empty record
                return;
            } else if (k == ValueNull.DELETED) {
                // found a deleted record
            } else if (k.equals(key)) {
                // found the record
                keys[index] = ValueNull.DELETED;
                values[index] = null;
                deletedCount++;
                size--;
                return;
            }
            index = (index + plus++) & mask;
        } while (plus <= len);
        // not found
    }

    /**
     * Get the value for this key. This method returns null if the key was not
     * found.
     *
     * @param key the key
     * @return the value for the given key
     */
    public V get(Value key) {
        int index = getIndex(key);
        int plus = 1;
        do {
            Value k = keys[index];
            if (k == null) {
                // found an empty record
                return null;
            } else if (k == ValueNull.DELETED) {
                // found a deleted record
            } else if (k.equals(key)) {
                // found it
                return values[index];
            }
            index = (index + plus++) & mask;
        } while (plus <= len);
        return null;
    }

    /**
     * Get the list of keys.
     *
     * @return all keys
     */
    public ArrayList<Value> keys() {
        ArrayList<Value> list = new ArrayList<>(size);
        for (Value k : keys) {
            if (k != null && k != ValueNull.DELETED) {
                list.add(k);
            }
        }
        return list;
    }

    /**
     * Get the list of values.
     *
     * @return all values
     */
    public ArrayList<V> values() {
        ArrayList<V> list = new ArrayList<>(size);
        int len = keys.length;
        for (int i = 0; i < len; i++) {
            Value k = keys[i];
            if (k != null && k != ValueNull.DELETED) {
                list.add(values[i]);
            }
        }
        return list;
    }

}

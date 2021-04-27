/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.vault.common;

import java.util.Collections;
import java.util.Comparator;
import org.apache.ignite.lang.ByteArray;

/**
 * Watch for vault entries.
 * Could be specified by range of keys.
 * If value of key in range is changed, then corresponding listener will be triggered.
 */
public final class VaultWatch {
    /** Comparator for {@code ByteArray} values. */
    private static final Comparator<ByteArray> CMP = ByteArray::compare;

    /**
     * Start key of range (inclusive).
     * If value of key in range is changed, then corresponding listener will be triggered.
     */
    private final ByteArray startKey;

    /**
     * End key of range (exclusive).
     * If value of key in range is changed, then corresponding listener will be triggered.
     */
    private final ByteArray endKey;

    /** Listener for vault's values updates. */
    private VaultListener listener;

    /**
     * @param startKey Start key of range (inclusive).
     * @param endkey End key of range (exclusive).
     * @param listener Listener.
     */
    public VaultWatch(ByteArray startKey, ByteArray endkey, VaultListener listener) {
        this.startKey = startKey;
        this.endKey = endkey;
        this.listener = listener;
    }

    /**
     * Notifies specified listener if {@code val} of key in range was changed.
     *
     * @param val Vault entry.
     * @return {@code True} if watch must continue event handling according to corresponding listener logic. If returns
     * {@code false} then the listener and corresponding watch will be unregistered.
     */
    public boolean notify(Entry val) {
        if (startKey != null && CMP.compare(val.key(), startKey) < 0)
            return true;

        if (endKey != null && CMP.compare(val.key(), endKey) >= 0)
            return true;

        return listener.onUpdate(Collections.singleton(val));
    }

    /**
     * The method will be called in case of an error occurred. The watch and corresponding listener will be
     * unregistered.
     *
     * @param e Exception.
     */
    public void onError(Throwable e) {
        listener.onError(e);
    }
}

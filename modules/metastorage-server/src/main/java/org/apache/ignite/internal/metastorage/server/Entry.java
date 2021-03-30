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

package org.apache.ignite.internal.metastorage.server;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Represents a storage unit as entry with key, value and revision, where
 * <ul>
 *     <li>key - an unique entry's key represented by an array of bytes. Keys are comparable in lexicographic manner.</li>
 *     <li>value - a data which is associated with a key and represented as an array of bytes.</li>
 *     <li>revision - a number which denotes a version of whole meta storage.
 *     Each change (which could include multiple entries) increments the revision. </li>
 *     <li>updateCounter - a number which increments on every update in the change under one revision.</li>
 * </ul>
 *
 * Instance of {@link #Entry} could represents:
 * <ul>
 *     <li>A regular entry which stores a particular key, a value and a revision number.</li>
 *     <li>An empty entry which denotes absence a regular entry in the meta storage for a given key.
 *     A revision is 0 for such kind of entry.</li>
 *     <li>A tombstone entry which denotes that a regular entry for a given key was removed from storage on some revision.</li>
 * </ul>
 */
public class Entry {
    /** Entry key. Couldn't be {@code null}. */
    @NotNull
    private final byte[] key;

    /**
     * Entry value.
     * <p>
     *     {@code val == null} only for {@link #empty()} and {@link #tombstone()} entries.
     * </p>
     */
    @Nullable
    private final byte[] val;

    /**
     * Revision number corresponding to this particular entry.
     * <p>
     *     {@code rev == 0} for {@link #empty()} entry,
     *     {@code rev > 0} for regular and {@link #tombstone()} entries.
     * </p>
     */
    private final long rev;

    /**
     * Update counter corresponds to this particular entry.
     * <p>
     *     {@code updCntr == 0} for {@link #empty()} entry,
     *     {@code updCntr > 0} for regular and {@link #tombstone()} entries.
     * </p>
     */
    private final long updCntr;

    /**
     * Constructor.
     *
     * @param key Key bytes. Couldn't be {@code null}.
     * @param val Value bytes. Couldn't be {@code null}.
     * @param rev Revision.
     * @param updCntr Update counter.
     */
    // TODO: It seems user will never create Entry, so we can reduce constructor scope to protected or package-private and reuse it from two-place private constructor.
    public Entry(@NotNull byte[] key, @NotNull byte[] val, long rev, long updCntr) {
        assert key != null : "key can't be null";
        assert val != null : "value can't be null";

        this.key = key;
        this.val = val;
        this.rev = rev;
        this.updCntr = updCntr;
    }

    /**
     * Constructor for empty and tombstone entries.
     *
     * @param key Key bytes. Couldn't be {@code null}.
     * @param rev Revision.
     * @param updCntr Update counter.
     */
    private Entry(@NotNull byte[] key, long rev, long updCntr) {
        assert key != null : "key can't be null";

        this.key = key;
        this.val = null;
        this.rev = rev;
        this.updCntr = updCntr;
    }

    /**
     * Creates an instance of empty entry for a given key.
     *
     * @param key Key bytes. Couldn't be {@code null}.
     * @return Empty entry.
     */
    @NotNull
    public static Entry empty(byte[] key) {
        return new Entry(key, 0, 0);
    }

    /**
     * Creates an instance of tombstone entry for a given key and a revision.
     *
     * @param key Key bytes. Couldn't be {@code null}.
     * @param rev Revision.
     * @param updCntr Update counter.
     * @return Empty entry.
     */
    @NotNull
    public static Entry tombstone(byte[] key, long rev, long updCntr) {
        assert rev > 0 : "rev must be positive for tombstone entry.";
        assert updCntr > 0 : "updCntr must be positive for tombstone entry.";

        return new Entry(key, rev, updCntr);
    }

    /**
     * Returns a key.
     *
     * @return Key.
     */
    @NotNull
    public byte[] key() {
        return key;
    }

    /**
     * Returns a value.
     *
     * @return Value.
     */
    @Nullable
    public byte[] value() {
        return val;
    }

    /**
     * Returns a revision.
     *
     * @return Revision.
     */
    public long revision() {
        return rev;
    }

    /**
     * Returns a update counter.
     *
     * @return Update counter.
     */
    public long updateCounter() {
        return updCntr;
    }

    /**
     * Returns value which denotes whether entry is tombstone or not.
     *
     * @return {@code True} if entry is tombstone, otherwise - {@code false}.
     */
    public boolean tombstone() {
        return val == null && rev > 0 && updCntr > 0;
    }

    /**
     * Returns value which denotes whether entry is empty or not.
     *
     * @return {@code True} if entry is empty, otherwise - {@code false}.
     */
    public boolean empty() {
        return val == null && rev == 0 && updCntr == 0;
    }
}

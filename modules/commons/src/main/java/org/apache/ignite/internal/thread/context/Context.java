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

package org.apache.ignite.internal.thread.context;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.thread.context.Scope.NOOP_SCOPE;

/**
 * Represents a storage of {@link ContextAttribute}s and their corresponding values bound to the thread.
 * The state of Context is determined by a sequence of {@link Update}s applied to it. Each Update stores the
 * updated or newly added {@link ContextAttribute} values and link to the previous Update.
 * <pre>
 *         +-----------+   +-----------+
 *         |           |   | A1 -> V2  |
 * null <--| A1 -> V1  |<--|           |
 *         |           |   | A2 -> V3  |
 *         +-----------+   +-----------+
 *</pre>
 * Context Updates can be undone in the same order they were applied by closing the {@link Scope} associated with each
 * update (see {@link #set(ContextAttribute, Object)} and related methods).
 */
public class Context {
    /** */
    private static final ThreadLocal<Context> INSTANCE = ThreadLocal.withInitial(Context::new);

    /**
     * Sequence of updated applied to the Context. Each update holds a link to the previous Update, so we store only
     * the reference to the last one.
     */
    @Nullable private Update lastUpd;

    /** */
    private Context() {
        // No-op.
    }

    /**
     * Retrieves value associated with specified attribute by accessing Context bound to the thread this method is
     * called from. If no value is explicitly associated with specified attribute, {@link ContextAttribute#initialValue()}
     * is returned.
     *
     * @param attr Context Attribute.
     * @return Context Attribute Value.
     */
    @Nullable public static <T> T get(ContextAttribute<T> attr) {
        return INSTANCE.get().getInternal(attr);
    }

    /**
     * Updates the value of the specified attribute for the Context bound to the thread this method is called from.
     *
     * @param attr Context Attribute.
     * @return Scope instance that, when closed, undoes the applied update. It is crucial to undo all applied Context
     * updates to free up thread-bound resources and avoid memory leaks, so it is highly encouraged to use a
     * try-with-resource block to close the returned Scope. Note, updates must be undone in the same order they were applied.
     */
    public static <T> Scope set(ContextAttribute<T> attr, T val) {
        Context ctx = INSTANCE.get();

        return ctx.getInternal(attr) == val ? NOOP_SCOPE : ctx.applyAttributeUpdates(new AttributeValueHolder<>(attr, val));
    }

    /**
     * Updates the values of the specified attributes for the Context bound to the thread this method is called from.
     *
     * @param attr1 First Context Attribute.
     * @param val1 Values associated with first Context Attribute.
     * @param attr2 Second Context Attribute.
     * @param val2 Values associated with second Context Attribute.
     * @return Scope instance that, when closed, undoes the applied update. It is crucial to undo all applied Context
     * updates to free up thread-bound resources and avoid memory leaks, so it is highly encouraged to use a
     * try-with-resource block to close the returned Scope. Note, updates must be undone in the same order they were applied.
     */
    public static <T1, T2> Scope set(
        ContextAttribute<T1> attr1, T1 val1,
        ContextAttribute<T2> attr2, T2 val2
    ) {
        return ContextUpdater.create().set(attr1, val1).set(attr2, val2).apply();
    }

    /**
     * Updates the values of the specified attributes for the Context bound to the thread this method is called from.
     *
     * @param attr1 First Context Attribute.
     * @param val1 Values associated with first Context Attribute.
     * @param attr2 Second Context Attribute.
     * @param val2 Values associated with second Context Attribute.
     * @param attr3 Third Context Attribute.
     * @param val3 Values associated with third Context Attribute.
     * @return Scope instance that, when closed, undoes the applied update. It is crucial to undo all applied Context
     * updates to free up thread-bound resources and avoid memory leaks, so it is highly encouraged to use a
     * try-with-resource block to close the returned Scope. Note, updates must be undone in the same order they were applied.
     */
    public static <T1, T2, T3> Scope set(
        ContextAttribute<T1> attr1, T1 val1,
        ContextAttribute<T2> attr2, T2 val2,
        ContextAttribute<T3> attr3, T3 val3
    ) {
        return ContextUpdater.create().set(attr1, val1).set(attr2, val2).set(attr3, val3).apply();
    }

    /**
     * Creates Snapshot of all attributes and their corresponding values stored in the Context bound to the thread this
     * method is called from.
     *
     * @return Context Snapshot.
     */
    public static ContextSnapshot createSnapshot() {
        return INSTANCE.get().createSnapshotInternal();
    }

    /**
     * Restores values of all attributes for Context bound to the thread this method is called from.
     *
     * @param snp Context Snapshot.
     * @return Scope instance that, when closed, undoes the applied operation. It is crucial to undo all applied Context
     * updates to free up thread-bound resources and avoid memory leaks, so it is highly encouraged to use a
     * try-with-resource block to close the returned Scope. Note, updates must be undone in the same order they were applied.
     */
    public static Scope restoreSnapshot(ContextSnapshot snp) {
        return INSTANCE.get().restoreSnapshotInternal(snp);
    }

    /**
     * Retrieves value for the specified attribute from the current Context. If no value is explicitly associated with
     * specified attribute, {@link ContextAttribute#initialValue()} is returned.
     */
    @Nullable private <T> T getInternal(ContextAttribute<T> attr) {
        if (lastUpd == null || (lastUpd.storedAttrBits & attr.bitmask()) == 0)
            return attr.initialValue(); // Context does not store value for the specified attribute.

        AttributeValueHolder<T> valHolder = findAttributeValue(attr);

        assert valHolder != null;
        assert valHolder.attribute().equals(attr);

        return valHolder.value();
    }

    /** Updates the current context with the specified attributes and their corresponding values. */
    private Scope applyAttributeUpdates(AttributeValueHolder<?>... attrVals) {
        lastUpd = new Update(attrVals, lastUpd);

        return lastUpd;
    }

    /** Undoes the latest updated. */
    private void undo(Update upd) {
        assert lastUpd == upd;

        lastUpd = lastUpd.prev;
    }

    /** Iterates over the currently applied context updates and finds the latest value associated with the specified attribute. */
    private <T> AttributeValueHolder<T> findAttributeValue(ContextAttribute<T> attr) {
        for (Update upd = lastUpd; upd != null; upd = upd.prev) {
            if (!upd.holdsValueFor(attr))
                continue;

            return upd.value(attr);
        }

        return null;
    }

    /** */
    private ContextSnapshot createSnapshotInternal() {
        // The sequence of updates defines the state of the Context. Each update is linked to the previous one and immutable.
        // Therefore, to restore the context state elsewhere, we only need to share a reference to the most recent update.
        return lastUpd;
    }

    /** */
    private Scope restoreSnapshotInternal(ContextSnapshot newSnp) {
        ContextSnapshot prevSnp = createSnapshotInternal();

        if (newSnp == prevSnp)
            return NOOP_SCOPE;

        changeState(prevSnp, newSnp);

        return () -> INSTANCE.get().changeState(newSnp, prevSnp);
    }

    /** */
    private void changeState(ContextSnapshot expState, ContextSnapshot newState) {
        assert lastUpd == expState;

        lastUpd = (Update)newState;
    }

    /** Represents Update applied to the Context. */
    private static class Update implements Scope, ContextSnapshot {
        /** Updated attributes and their corresponding values. */
        private final AttributeValueHolder<?>[] attrVals;

        /**
         * Bits representing all attributes which values were changed by this update.
         *
         * @see ContextAttribute#bitmask()
         */
        private final int updAttrBits;

        /**
         * Bits representing all attributes stored in the current Context after this Update and all preceding are applied.
         * We need this for two purposes:
         * <ul>
         * <li>fast check whether any of the currently applied Context Updates store value for the particular attribute</li>
         * <li>do not recalculate state of all attributes when update is undone</li>
         * </ul>
         *
         * @see ContextAttribute#bitmask()
         */
        private final int storedAttrBits;

        /** Link to the previous update. */
        private final Update prev;

        /** */
        Update(AttributeValueHolder<?>[] attrVals, Update prev) {
            this.attrVals = attrVals;
            this.prev = prev;

            updAttrBits = mergeUpdatedAttributeBits(attrVals);
            storedAttrBits = prev == null ? updAttrBits : prev.storedAttrBits | updAttrBits;
        }

        /** @return Whether current update contains value for the specified attribute. */
        boolean holdsValueFor(ContextAttribute<?> attr) {
            return (updAttrBits & attr.bitmask()) != 0;
        }

        /**
         * @return Attribute value that was set by the current update for the specified attribute. {@code null} if
         * specified Attribute was not changed by this update.
         */
        @Nullable <T> AttributeValueHolder<T> value(ContextAttribute<T> attr) {
            // We iterate in reverse order to correctly handle the case when the value for the same attribute is
            // specified multiple times.
            for (int i = attrVals.length - 1; i >= 0; i--) {
                AttributeValueHolder<?> valHolder = attrVals[i];

                if (valHolder.attribute().equals(attr))
                    return ((AttributeValueHolder<T>)valHolder);
            }

            return null;
        }

        /** */
        private static int mergeUpdatedAttributeBits(AttributeValueHolder<?>[] attrVals) {
            int res = 0;

            for (AttributeValueHolder<?> attrVal : attrVals)
                res |= attrVal.attribute().bitmask();

            return res;
        }

        /** */
        @Override public void close() {
            INSTANCE.get().undo(this);
        }
    }

    /** Allows to change multiple attribute values in a single update operation and skip updates that changes nothing. */
    private static class ContextUpdater {
        /** */
        private static final int INIT_UPDATES_CAPACITY = 3;

        /** */
        private final Context ctx;

        /** */
        private List<AttributeValueHolder<?>> updates;

        /** */
        private ContextUpdater(Context ctx) {
            this.ctx = ctx;
        }

        /** */  
        <T> ContextUpdater set(ContextAttribute<T> attr, T val) {
            if (ctx.getInternal(attr) == val)
                return this;

            if (updates == null)
                updates = new ArrayList<>(INIT_UPDATES_CAPACITY);

            updates.add(new AttributeValueHolder<>(attr, val));

            return this;
        }

        /** */
        Scope apply() {
            if (F.isEmpty(updates))
                return NOOP_SCOPE;

            AttributeValueHolder<?>[] sealedUpdates = new AttributeValueHolder[updates.size()];

            updates.toArray(sealedUpdates);

            return ctx.applyAttributeUpdates(sealedUpdates);
        }

        /** */
        static ContextUpdater create() {
            return new ContextUpdater(INSTANCE.get());
        }
    }
}

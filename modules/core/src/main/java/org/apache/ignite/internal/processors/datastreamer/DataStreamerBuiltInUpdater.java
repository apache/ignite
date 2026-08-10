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

package org.apache.ignite.internal.processors.datastreamer;

import java.util.function.Supplier;
import org.apache.ignite.stream.StreamReceiver;
import org.jetbrains.annotations.Nullable;

/**
 * The built-in updaters. Every node has them, so a request names the one it needs instead of carrying a serialized
 * copy. Both the updater and its message are built on demand: a node that streams with a custom receiver, or does not
 * stream at all, builds neither.
 */
enum DataStreamerBuiltInUpdater {
    /** {@link DataStreamerImpl#ISOLATED_UPDATER}. */
    ISOLATED(() -> DataStreamerImpl.ISOLATED_UPDATER),

    /** {@link DataStreamerCacheUpdaters#individual()}. */
    INDIVIDUAL(DataStreamerCacheUpdaters::individual),

    /** {@link DataStreamerCacheUpdaters#batched()}. */
    BATCHED(DataStreamerCacheUpdaters::batched),

    /** {@link DataStreamerCacheUpdaters#batchedSorted()}. */
    BATCHED_SORTED(DataStreamerCacheUpdaters::batchedSorted);

    /** */
    private final Supplier<StreamReceiver<?, ?>> updaterSupplier;

    /** */
    private StreamReceiver<?, ?> updater;

    /** */
    private DataStreamerReceiverMessage msg;

    /** @param updaterSupplier Supplier of the updater this constant stands for. */
    DataStreamerBuiltInUpdater(Supplier<StreamReceiver<?, ?>> updaterSupplier) {
        this.updaterSupplier = updaterSupplier;
    }

    /** @return Updater of this node. */
    StreamReceiver<?, ?> updater() {
        if (updater == null)
            updater = updaterSupplier.get();

        return updater;
    }

    /** @return Message naming this updater; one per constant, as a built-in updater never changes. */
    DataStreamerReceiverMessage message() {
        if (msg == null)
            msg = new DataStreamerReceiverMessage(this);

        return msg;
    }

    /**
     * @param updater Updater to look up.
     * @return Constant standing for {@code updater}, or {@code null} when it is a custom one.
     */
    static @Nullable DataStreamerBuiltInUpdater of(StreamReceiver<?, ?> updater) {
        for (DataStreamerBuiltInUpdater builtIn : values()) {
            if (builtIn.updater() == updater)
                return builtIn;
        }

        return null;
    }
}

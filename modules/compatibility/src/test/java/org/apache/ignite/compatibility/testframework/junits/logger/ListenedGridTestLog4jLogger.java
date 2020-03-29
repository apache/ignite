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

package org.apache.ignite.compatibility.testframework.junits.logger;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.testframework.junits.logger.GridTestLog4jLogger;
import org.apache.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Listened version of {@link GridTestLog4jLogger} logger. Provides methods of registering and notifying listeners.
 */
public class ListenedGridTestLog4jLogger extends GridTestLog4jLogger {
    /** Listeners. */
    private final ConcurrentMap<UUID, IgniteInClosure<String>> lsnrs = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    public ListenedGridTestLog4jLogger(Logger impl) {
        super(impl);
    }

    /**
     * Creates new logger instance with given category.
     *
     * @param ctgr Category.
     * @return Initiated logger.
     */
    public static GridTestLog4jLogger createLogger(Object ctgr) {
        return new ListenedGridTestLog4jLogger(ctgr == null ? Logger.getRootLogger() :
            ctgr instanceof Class ? Logger.getLogger(((Class<?>)ctgr).getName()) :
                Logger.getLogger(ctgr.toString()));
    }

    /** {@inheritDoc} */
    @Override public void info(String msg) {
        notifyListeners(msg);

        super.info(msg);
    }

    /**
     * Notifies registered listeners.
     *
     * @param msg Message.
     */
    protected void notifyListeners(String msg) {
        for (IgniteInClosure<String> lsnr : lsnrs.values())
            lsnr.apply(msg);
    }

    /**
     * Adds listener.
     *
     * @param key Key.
     * @param lsnr Listener.
     * @return The previous value associated with the specified key, or {@code null} if there was no mapping for the key.
     */
    @Nullable public IgniteInClosure<String> addListener(@NotNull UUID key, @NotNull IgniteInClosure<String> lsnr) {
        return lsnrs.putIfAbsent(key, lsnr);
    }

    /**
     * Removes listeners.
     *
     * @param key Key.
     * @return Returns the value to which this map previously associated the key, or {@code null} if the map contained no
     * mapping for the key.
     */
    @Nullable public IgniteInClosure<String> removeListener(@NotNull UUID key) {
        return lsnrs.remove(key);
    }
}

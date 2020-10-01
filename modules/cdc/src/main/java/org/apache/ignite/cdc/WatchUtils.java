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

package org.apache.ignite.cdc;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;

public class WatchUtils {
    /** Logger */
    private final IgniteLogger log;

    /**
     * @param log Logger.
     */
    public WatchUtils(IgniteLogger log) {
        this.log = log;
    }

    /**
     * Waits for creation of the path and notifies callback of it.
     *
     * @param watchDir Dir to watch
     * @param filter Filter of events.
     * @param callback Callback to be notified.
     */
    public void waitFor(Path watchDir, Predicate<Path> filter, Consumer<Path> callback) {
        waitFor(watchDir, filter, p -> {
            callback.accept(p);

            return false;
        });
    }

    /**
     * Waits for creation of the path and notifies callback of it.
     *
     * @param watchDir Dir to watch
     * @param filter Filter of events.
     * @param callback Callback to be notified.
     */
    public void waitFor(Path watchDir, Predicate<Path> filter, Predicate<Path> callback) {
        try {
            try(Stream<Path> children = Files.walk(watchDir, 1).filter(p -> !p.equals(watchDir))) {
                final boolean[] status = {true};

                children.filter(filter).sorted().peek(p -> {
                    if (status[0])
                        status[0] = callback.test(p);
                }).count();

                if (!status[0])
                    return;
            }

            if (log.isInfoEnabled())
                log.info("Waiting for creation of directory. Watching directory[dir=" + watchDir + ']');

            try (WatchService watcher = FileSystems.getDefault().newWatchService()) {
                watchDir.register(watcher, ENTRY_CREATE);

                boolean needNext = true;

                while (needNext) {
                    WatchKey key = watcher.take();

                    for (WatchEvent<?> evt: key.pollEvents()) {
                        WatchEvent.Kind<?> kind = evt.kind();

                        if (kind == OVERFLOW)
                            continue;

                        Path evtPath = Paths.get(watchDir.toString(), evt.context().toString()).toAbsolutePath();

                        if (log.isInfoEnabled())
                            log.info("Event received[evt=" + evtPath.toAbsolutePath() + ",kind=" + kind + ']');

                        if (filter.test(evtPath)) {
                            needNext = callback.test(evtPath);

                            if (!needNext)
                                break;
                        }
                    }

                    if (!needNext)
                        break;

                    boolean reset = key.reset();

                    if (!reset)
                        throw new IllegalStateException("Key no longer valid.");
                }
            }
        }
        catch (IOException | InterruptedException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Waits for creation of the path and notifies callback of it.
     *
     * @param waitFor Path to wait for.
     * @param callback Callback to be notified.
     */
    public void waitFor(Path waitFor, Consumer<Path> callback) {
        waitFor(waitFor.getParent(), waitFor::equals, callback);
    }
}

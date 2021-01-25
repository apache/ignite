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
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.cdc.IgniteCDC.waitFor;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** Tests watch method. */
public class WatchSelfTest extends GridCommonAbstractTest {
    /** */
    private Path root;

    /** */
    private Path notExists;

    /** */
    private Path parent;

    /** */
    private Path file;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        root = Files.createTempDirectory("watch_self_test");

        notExists = root.resolve("a");

        parent = notExists.resolve("b");

        file = parent.resolve("my_file");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
            @Override public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                if (exc != null) {
                    throw exc;
                }
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    /** */
    @Test
    public void testWaitIfParentNotExists() throws Exception {
        final boolean[] found = {false};

        IgniteInternalFuture<?> fut = runAsync(() -> {
            try {
                waitFor(parent, file::equals, Path::compareTo, p -> {
                    found[0] = p.equals(file);
                    return !found[0];
                }, log());
            }
            catch (InterruptedException e) {
                throw new RuntimeException();
            }
        });

        Files.createDirectories(parent);
        Files.createFile(parent.resolve("another_file"));
        Files.createFile(file);

        waitForCondition(() -> found[0], getTestTimeout());

        fut.cancel();
    }

    @Test
    public void testCallbackIfAlreadyExists() throws Exception {
        Files.createDirectories(parent);
        Files.createFile(parent.resolve("another_file"));
        Files.createFile(file);

        final boolean[] found = {false};

        IgniteInternalFuture<?> fut = runAsync(() -> {
            try {
                waitFor(parent, file::equals, Path::compareTo, p -> {
                    found[0] = p.equals(file);
                    return !found[0];
                }, log());
            }
            catch (InterruptedException e) {
                throw new RuntimeException();
            }
        });

        waitForCondition(() -> found[0], getTestTimeout());

        fut.cancel();
    }

    @Test
    public void testMultipleCallbacks() throws Exception {
        Files.createDirectories(parent);

        String ext = "txt";
        int expCnt = 3;

        final int[] cnt = {0};

        IgniteInternalFuture<?> fut = runAsync(() -> {
            try {
                waitFor(parent, p -> p.toString().endsWith(ext),
                    Path::compareTo, p -> {
                    if (p.toString().endsWith(ext))
                        cnt[0]++;

                    return cnt[0] < expCnt;
                }, log());
            }
            catch (InterruptedException e) {
                throw new RuntimeException();
            }
        });

        runAsync(() -> {
            try {
                for (int i = 0; i < expCnt; i++)
                    Files.createFile(parent.resolve("my_file" + i + "." + ext));
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        waitForCondition(() -> cnt[0] == expCnt, getTestTimeout());

        fut.cancel();
    }

    @Test
    public void testCallbackAfterRemove() throws Exception {
        Files.createDirectories(parent);

        String ext = "txt";
        int expCnt = 6;

        final int[] cnt = {0};

        for (int i = 0; i < expCnt; i++)
            Files.createFile(parent.resolve("my_file" + i + "." + ext));

        IgniteInternalFuture<?> fut = runAsync(() -> {
            try {
                waitFor(parent, p -> p.toString().endsWith(ext),
                    Path::compareTo, p -> {
                        if (p.toString().endsWith(ext))
                            cnt[0]++;

                        return cnt[0] < expCnt;
                    }, log());
            }
            catch (InterruptedException e) {
                throw new RuntimeException();
            }
        });

        runAsync(() -> {
            try {
                for (int i = 0; i < expCnt; i++) {
                    Files.delete(parent.resolve("my_file" + i + "." + ext));
                    Files.createFile(parent.resolve("my_file" + i + "." + ext));
                }
            }
            catch (IOException e) {
                throw new RuntimeException();
            }
        });

        waitForCondition(() -> cnt[0] == expCnt, getTestTimeout());

        fut.cancel();
    }
}

/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.yardstick.io;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;

/**
 * Utility class for working with files.
 */
public class FileUtils {
    /**
     * Clean directory.
     *
     * @param path path to directory.
     */
    public static void cleanDirectory(String path) throws IOException {
        LinkedList<Path> paths = new LinkedList<>();

        appendOrRemove(paths, Files.newDirectoryStream(Paths.get(path)));

        while (!paths.isEmpty()) {
            if (Files.newDirectoryStream(paths.getLast()).iterator().hasNext())
                appendOrRemove(paths, Files.newDirectoryStream(paths.getLast()));
            else
                Files.delete(paths.removeLast());
        }
    }

    /**
     * Add path to the stack if path is directory, otherwise delete it.
     *
     * @param paths Stack of paths.
     * @param ds Stream of paths.
     */
    private static void appendOrRemove(LinkedList<Path> paths, DirectoryStream<Path> ds) throws IOException {
        for (Path p : ds) {
            if (Files.isDirectory(p))
                paths.add(p);
            else
                Files.delete(p);
        }
    }
}


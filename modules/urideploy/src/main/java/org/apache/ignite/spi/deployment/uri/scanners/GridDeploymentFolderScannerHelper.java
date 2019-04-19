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

package org.apache.ignite.spi.deployment.uri.scanners;

import java.io.File;
import java.io.FileFilter;

/**
 * Helper class that recursively goes through the list of
 * files/directories and handles them by calling given handler.
 */
public final class GridDeploymentFolderScannerHelper {
    /**
     * Enforces singleton.
     */
    private GridDeploymentFolderScannerHelper() {
        // No-op.
    }

    /**
     * Applies given file to the handler if it is not filtered out by given
     * filter. If given file is not accepted by filter and it is a directory
     * than recursively scans directory and apply the same method for every
     * found file.
     *
     * @param file File that should be handled.
     * @param filter File filter.
     * @param handler Handler which should handle files.
     */
    public static void scanFolder(File file, FileFilter filter, GridDeploymentFileHandler handler) {
        assert file != null;

        if (filter.accept(file))
            handler.handle(file);
        else if (file.isDirectory()) {
            for (File child : file.listFiles()) {
                scanFolder(child, filter, handler);
            }
        }
    }
}
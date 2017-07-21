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

package org.apache.ignite.spi.deployment.uri.scanners;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URI;
import org.apache.ignite.IgniteLogger;

/**
 * Deployment scanner context.
 */
public interface UriDeploymentScannerContext {
    /**
     * Creates temp file in temp directory.
     *
     * @param fileName File name.
     * @param tmpDir dir to creating file.
     * @return created file.
     * @throws IOException if error occur.
     */
    File createTempFile(String fileName, File tmpDir) throws IOException;

    /**
     * Gets temporary deployment directory.
     *
     * @return Temporary deployment directory.
     */
    File getDeployDirectory();

    /**
     * Gets filter for found files. Before listener is notified about
     * changes with certain file last should be accepted by filter.
     *
     * @return New, updated or deleted file filter.
     */
    FilenameFilter getFilter();

    /**
     * Gets deployment listener.
     *
     * @return Listener which should be notified about all deployment events
     *      by scanner.
     */
    GridUriDeploymentScannerListener getListener();

    /**
     * Gets scanner logger.
     *
     * @return Logger.
     */
    IgniteLogger getLogger();

    /**
     * Gets deployment URI.
     *
     * @return Deployment URI.
     */
    URI getUri();

    /**
     * Tests whether scanner was cancelled before or not.
     *
     * @return {@code true} if scanner was cancelled and {@code false}
     *      otherwise.
     */
    boolean isCancelled();

    /**
     * Tests whether first scan completed or not.
     *
     * @return {@code true} if first scan has been already completed and
     *      {@code false} otherwise.
     */
    boolean isFirstScan();
}
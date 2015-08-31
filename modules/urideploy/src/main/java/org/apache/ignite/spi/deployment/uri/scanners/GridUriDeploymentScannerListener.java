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
import java.util.EventListener;
import java.util.List;

/**
 * Scanner listener interface. Whatever deployment scanner is used
 * (http, file and so on) following events happens:
 * <ul>
 * <li>{@code onNewOrUpdatedFile} - happens when new file has been found or updated.</li>
 * <li>{@code onDeletedFiles} - happens when file(s) has been removed.</li>
 * <li>{@code onFirstScanFinished} - happens when scanner completed its first scan.</li>
 * </ul>
 */
public interface GridUriDeploymentScannerListener extends EventListener {
    /**
     * Notifies about new file or those one that was updated.
     *
     * @param file New or updated file.
     * @param uri File URI.
     * @param tstamp File modification date.
     */
    public void onNewOrUpdatedFile(File file, String uri, long tstamp);

    /**
     * Notifies about removed files.
     *
     * @param uris List of removed files.
     */
    public void onDeletedFiles(List<String> uris);

    /**
     * Notifies about first scan completion.
     */
    public void onFirstScanFinished();
}
/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi.deployment.uri.scanners;

import java.io.*;
import java.util.*;

/**
 * Scanner listener interface. Whatever deployment scanner is used
 * (ftp, http, file and so on) following events happens:
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

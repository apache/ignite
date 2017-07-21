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

package org.apache.ignite.yarn;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.ignite.yarn.utils.IgniteYarnUtils;

/**
 * Downloads and stores Ignite.
 */
public class IgniteProvider {
    /** */
    public static final String DOWNLOAD_LINK = "http://tiny.cc/updater/download_community.php";

    /** */
    private ClusterProperties props;

    /** */
    private String latestVersion = null;

    /** */
    private boolean hdfs = false;

    /** */
    private FileSystem fs;

    /**
     * @param props Cluster properties.
     * @param fs Hadoop file system.
     */
    public IgniteProvider(ClusterProperties props, FileSystem fs) {
        this.props = props;
        this.fs = fs;
    }

    /**
     * @return Latest ignite version.
     */
    public Path getIgnite() throws Exception {
        File folder = checkDownloadFolder();

        if (latestVersion == null) {
            List<String> localFiles = findIgnites(folder);
            List<String> hdfsFiles = findIgnites(fs, props.igniteReleasesDir());

            String localLatestVersion = findLatestVersion(localFiles);
            String hdfsLatestVersion = findLatestVersion(hdfsFiles);

            if (localLatestVersion != null && hdfsLatestVersion != null) {
                if (VersionComparator.INSTANCE.compare(hdfsLatestVersion, localLatestVersion) >= 0) {
                    latestVersion = hdfsLatestVersion;

                    hdfs = true;
                }
            }
            else if (localLatestVersion != null)
                latestVersion = localLatestVersion;
            else if (hdfsLatestVersion != null) {
                latestVersion = hdfsLatestVersion;

                hdfs = true;
            }
        }

        String newVersion = updateIgnite(latestVersion);

        if (latestVersion != null && newVersion.equals(latestVersion)) {
            if (hdfs)
                return new Path(formatPath(props.igniteReleasesDir(), latestVersion));
            else
                return IgniteYarnUtils.copyLocalToHdfs(fs, formatPath(props.igniteLocalWorkDir(), latestVersion),
                    formatPath(props.igniteReleasesDir(), latestVersion));
        }
        else {
            latestVersion = newVersion;

            return IgniteYarnUtils.copyLocalToHdfs(fs, formatPath(props.igniteLocalWorkDir(), latestVersion),
                formatPath(props.igniteReleasesDir(), latestVersion));
        }
    }

    /**
     * @param folder Folder.
     * @return Ignite archives.
     */
    private List<String> findIgnites(File folder) {
        String[] files = folder.list();

        List<String> ignites = new ArrayList<>();

        if (files != null) {
            for (String fileName : files) {
                if (fileName.contains("gridgain-community-fabric-") && fileName.endsWith(".zip"))
                    ignites.add(fileName);
            }
        }

        return ignites;
    }

    /**
     * @param files Files.
     * @return latest ignite version.
     */
    private String findLatestVersion(List<String> files) {
        String latestVersion = null;

        if (!files.isEmpty()) {
            if (files.size() == 1)
                latestVersion = parseVersion(files.get(0));
            else
                latestVersion = parseVersion(Collections.max(files, VersionComparator.INSTANCE));
        }

        return latestVersion;
    }

    /**
     * @param fs File system,
     * @param folder Folder.
     * @return Ignite archives.
     */
    private List<String> findIgnites(FileSystem fs, String folder) {
        FileStatus[] fileStatuses = null;

        try {
            fileStatuses = fs.listStatus(new Path(folder));
        }
        catch (FileNotFoundException ignored) {
            // Ignore. Folder doesn't exist.
        }
        catch (Exception e) {
            throw new RuntimeException("Couldnt get list files from hdfs.", e);
        }

        List<String> ignites = new ArrayList<>();

        if (fileStatuses != null) {
            for (FileStatus file : fileStatuses) {
                String fileName = file.getPath().getName();

                if (fileName.contains("gridgain-community-fabric-") && fileName.endsWith(".zip"))
                    ignites.add(fileName);
            }
        }

        return ignites;
    }

    /**
     * @param version Ignite version.
     * @return Ignite.
     */
    public Path getIgnite(String version) throws Exception {
        checkDownloadFolder();

        // Download ignite.
        String fileName = downloadIgnite(version);

        Path dst = new Path(props.igniteReleasesDir() + File.separator + fileName);

        if (!fs.exists(dst))
            fs.copyFromLocalFile(new Path(props.igniteLocalWorkDir() + File.separator + fileName), dst);

        return dst;
    }

    /**
     * @param folder folder
     * @param version version
     * @return Path
     */
    private static String formatPath(String folder, String version) {
        return folder + File.separator + "gridgain-community-fabric-" + version + ".zip";
    }

    /**
     * @param currentVersion The current latest version.
     * @return Current version if the current version is latest; new ignite version otherwise.
     */
    private String updateIgnite(String currentVersion) {
        try {
            URL url;

            if (currentVersion == null)
                url = new URL(DOWNLOAD_LINK);
            else
                url = new URL(DOWNLOAD_LINK + "?version=" + currentVersion);

            HttpURLConnection conn = (HttpURLConnection)url.openConnection();

            int code = conn.getResponseCode();

            if (code == 200) {
                String redirectUrl = conn.getURL().toString();

                checkDownloadFolder();

                FileOutputStream outFile = new FileOutputStream(props.igniteLocalWorkDir() + File.separator
                    + fileName(redirectUrl));

                outFile.getChannel().transferFrom(Channels.newChannel(conn.getInputStream()), 0, Long.MAX_VALUE);

                outFile.close();

                return parseVersion(redirectUrl);
            }
            else if (code == 304)
                // This version is latest.
                return currentVersion;
            else
                throw new RuntimeException("Got unexpected response code. Response code: " + code);
        }
        catch (IOException e) {
            throw new RuntimeException("Failed update ignite.", e);
        }
    }

    /**
     * @param igniteUrl Url to ignite.
     * @return Ignite file name.
     */
    private String downloadIgnite(String igniteUrl) {
        try {
            URL url = new URL(igniteUrl);

            HttpURLConnection conn = (HttpURLConnection)url.openConnection();

            int code = conn.getResponseCode();

            if (code == 200) {
                String fileName = fileName(url.toString());

                String filePath = props.igniteLocalWorkDir() + File.separator + fileName;

                if (new File(filePath).exists())
                    return fileName;

                FileOutputStream outFile = new FileOutputStream(filePath);

                outFile.getChannel().transferFrom(Channels.newChannel(conn.getInputStream()), 0, Long.MAX_VALUE);

                outFile.close();

                return fileName;
            }
            else
                throw new RuntimeException("Got unexpected response code. Response code: " + code);
        }
        catch (IOException e) {
            throw new RuntimeException("Failed update ignite.", e);
        }
    }

    /**
     * @return Download folder.
     */
    private File checkDownloadFolder() {
        File file = new File(props.igniteLocalWorkDir());

        if (!file.exists())
            file.mkdirs();

        if (!file.exists())
            throw new RuntimeException("Couldn't create local directory! Path: " + file.toURI());

        return file;
    }

    /**
     * @param url URL.
     * @return Ignite version.
     */
    private static String parseVersion(String url) {
        String[] split = url.split("-");

        return split[split.length - 1].replaceAll(".zip", "");
    }

    /**
     * @param url URL.
     * @return File name.
     */
    private static String fileName(String url) {
        String[] split = url.split("/");

        return split[split.length - 1];
    }

    /**
     * Ignite version comparator.
     */
    public static final class VersionComparator implements Comparator<String> {
        /** */
        public static final VersionComparator INSTANCE = new VersionComparator();

        /** */
        private VersionComparator() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public int compare(String f1, String f2) {
            if (f1.equals(f2))
                return 0;

            String[] ver1 = parseVersion(f1).split("\\.");
            String[] ver2 = parseVersion(f2).split("\\.");

            if (Integer.valueOf(ver1[0]) >= Integer.valueOf(ver2[0])
                && Integer.valueOf(ver1[1]) >= Integer.valueOf(ver2[1])
                && Integer.valueOf(ver1[2]) >= Integer.valueOf(ver2[2]))

                return 1;
            else
                return -1;
        }
    }
}

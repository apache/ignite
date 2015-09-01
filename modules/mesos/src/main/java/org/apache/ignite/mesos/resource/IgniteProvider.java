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

package org.apache.ignite.mesos.resource;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Class downloads and stores Ignite.
 */
public class IgniteProvider {
    /** */
    public static final String DOWNLOAD_LINK = "http://tiny.cc/updater/download_community.php";

    /** */
    public static final String DIRECT_DOWNLOAD_LINK = "http://www.gridgain.com/media/gridgain-community-fabric-";

    /** */
    private String downloadFolder;

    /** */
    private String latestVersion = null;

    /**
     * @param downloadFolder Folder with ignite.
     */
    public IgniteProvider(String downloadFolder) {
        this.downloadFolder = downloadFolder;
    }

    /**
     * @return Latest ignite version.
     */
    public String getIgnite() {
        File folder = checkDownloadFolder();

        if (latestVersion == null) {
            List<String> files = findIgnites(folder);

            if (!files.isEmpty()) {
                if (files.size() == 1)
                    latestVersion = parseVersion(files.get(0));
                else
                    latestVersion = parseVersion(Collections.max(files, new Comparator<String>() {
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
                    }));
            }
        }

        latestVersion = updateIgnite(latestVersion);

        return "gridgain-community-fabric-" + latestVersion + ".zip";
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
     * @param version Ignite version.
     * @return Ignite.
     */
    public String getIgnite(String version) {
        File folder = checkDownloadFolder();

        String[] ignites = folder.list();

        String ignite = null;

        if (ignites != null) {
            for (String fileName : ignites) {
                if (fileName.equals("gridgain-community-fabric-" + version + ".zip"))
                    ignite = fileName;
            }
        }

        if (ignite != null)
            return ignite;

        return downloadIgnite(version);
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

                FileOutputStream outFile = new FileOutputStream(downloadFolder + "/" + fileName(redirectUrl));

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
     * @param version The current latest version.
     * @return Ignite archive.
     */
    public String downloadIgnite(String version) {
        try {
            URL url = new URL(DIRECT_DOWNLOAD_LINK + version + ".zip");

            HttpURLConnection conn = (HttpURLConnection)url.openConnection();

            int code = conn.getResponseCode();

            if (code == 200) {
                checkDownloadFolder();

                String fileName = fileName(url.toString());

                FileOutputStream outFile = new FileOutputStream(downloadFolder + fileName);

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
        File file = new File(downloadFolder);

        if (!file.exists())
            file.mkdirs();

        return file;
    }

    /**
     * @param url URL.
     * @return Ignite version.
     */
    public static String parseVersion(String url) {
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
}
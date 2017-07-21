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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import org.apache.ignite.mesos.ClusterProperties;

/**
 * Class downloads and stores Ignite.
 */
public class IgniteProvider {
    // This constants are set by maven-ant-plugin.
    /** */
    private static final String DOWNLOAD_LINK = "http://ignite.run/download_ignite.php";

    /** */
    private static final String DOWNLOAD_URL_PATTERN = "https://archive.apache.org/dist/ignite/%s/apache-ignite-fabric-%s-bin.zip";

    /** */
    private String downloadFolder;

    /**
     * @param downloadFolder Folder with ignite.
     */
    public IgniteProvider(String downloadFolder) {
        this.downloadFolder = downloadFolder;
    }

    /**
     * @param ver Ignite version.
     * @throws IOException If downloading failed.
     * @return Path to latest ignite.
     */
    public String getIgnite(String ver) throws IOException {
        return downloadIgnite(ver);
    }

    /**
     * @param ver Ignite version which will be downloaded. If {@code null} will download the latest ignite version.
     * @throws IOException If downloading failed.
     * @return Ignite archive.
     */
    public String downloadIgnite(String ver) throws IOException {
        assert ver != null;

        URL url;

        if (ver.equals(ClusterProperties.DEFAULT_IGNITE_VERSION)) {
            URL updateUrl = new URL(DOWNLOAD_LINK);

            HttpURLConnection conn = (HttpURLConnection)updateUrl.openConnection();

            int code = conn.getResponseCode();

            if (code == 200)
                url = conn.getURL();
            else
                throw new RuntimeException("Failed to download ignite distributive. Maybe set incorrect version? " +
                    "[resCode:" + code + ", ver: " + ver + "]");
        }
        else
            url = new URL(String.format(DOWNLOAD_URL_PATTERN, ver.replace("-incubating", ""), ver));

        return downloadIgnite(url);
    }

    /**
     * Downloads ignite by URL if this version wasn't downloaded before.
     *
     * @param url URL to Ignite.
     * @return File name.
     */
    private String downloadIgnite(URL url) {
        assert url != null;

        try {
            HttpURLConnection conn = (HttpURLConnection)url.openConnection();

            int code = conn.getResponseCode();

            if (code == 200) {
                checkDownloadFolder();

                String fileName = fileName(url.toString());

                if (fileExist(fileName))
                    return fileName;

                FileOutputStream outFile = new FileOutputStream(downloadFolder + fileName);

                outFile.getChannel().transferFrom(Channels.newChannel(conn.getInputStream()), 0, Long.MAX_VALUE);

                outFile.close();

                return fileName;
            }
            else
                throw new RuntimeException("Got unexpected response code. Response code: " + code);
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to download Ignite.", e);
        }
    }

    /**
     * Checks that file exists.
     *
     * @param fileName File name.
     * @return {@code True} if file exist otherwise {@code false}.
     */
    private boolean fileExist(String fileName) {
        String pathToIgnite = downloadFolder + (downloadFolder.endsWith("/") ? "" : '/') + fileName;

        return new File(pathToIgnite).exists();
    }

    /**
     * Copy file to working directory.
     *
     * @param filePath File path.
     * @return File name.
     * @throws IOException If coping failed.
     */
    String copyToWorkDir(String filePath) throws IOException {
        Path srcFile = Paths.get(filePath);

        if (Files.exists(srcFile)) {
            checkDownloadFolder();

            Path newDir = Paths.get(downloadFolder);

            Path fileName = srcFile.getFileName();

            Files.copy(srcFile, newDir.resolve(fileName), StandardCopyOption.REPLACE_EXISTING);

            return fileName.toString();
        }

        return null;
    }

    /**
     * @return Download folder.
     */
    private File checkDownloadFolder() {
        File file = new File(downloadFolder);

        if (!file.exists())
            file.mkdirs();

        if (!file.exists())
            throw new IllegalArgumentException("Failed to create working directory: " + downloadFolder);

        return file;
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
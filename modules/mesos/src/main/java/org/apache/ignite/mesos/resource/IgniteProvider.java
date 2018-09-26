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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.ignite.mesos.ClusterProperties;

import static org.apache.ignite.mesos.ClusterProperties.IGNITE_VERSION;

/**
 * Class downloads and stores Ignite.
 */
public class IgniteProvider {
    /** Logger. */
    private static final Logger log = Logger.getLogger(IgniteProvider.class.getSimpleName());

    // This constants are set by maven-ant-plugin.
    /** */
    private static final String DOWNLOAD_URL_PATTERN = "https://archive.apache.org/dist/ignite/%s/apache-ignite-fabric-%s-bin.zip";

    /** URL for request Ignite latest version. */
    private final static String IGNITE_LATEST_VERSION_URL = "https://ignite.apache.org/latest";

    /** Mirrors. */
    private static final String APACHE_MIRROR_URL = "https://www.apache.org/dyn/closer.cgi?as_json=1";

    /** Ignite on Apache URL path. */
    private static final String IGNITE_PATH = "/ignite/%s/apache-ignite-fabric-%s-bin.zip";

    /** Version pattern. */
    private static final Pattern VERSION_PATTERN = Pattern.compile("(?<=version=).*\\S+");

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
     * @return Path to latest ignite.
     * @throws IOException If downloading failed.
     */
    public String getIgnite(String ver) throws IOException {
        return downloadIgnite(ver);
    }

    /**
     * @param ver Ignite version which will be downloaded. If {@code null} will download the latest ignite version.
     * @return Ignite archive.
     * @throws IOException If downloading failed.
     */
    public String downloadIgnite(String ver) throws IOException {
        assert ver != null;

        URL url;

        // get the latest version.
        if (ver.equals(ClusterProperties.DEFAULT_IGNITE_VERSION)) {
            try {
                ver = findLatestVersion();

                // and try to retrieve from a mirror.
                url = new URL(String.format(findMirror() + IGNITE_PATH, ver, ver));
            }
            catch (Exception e) {
                // fallback to archive.
                url = new URL(String.format(DOWNLOAD_URL_PATTERN, ver, ver));
            }
        }
        else {
            // or from archive.
            url = new URL(String.format(DOWNLOAD_URL_PATTERN, ver, ver));
        }

        return downloadIgnite(url);
    }

    /**
     * Attempts to retrieve the preferred mirror.
     *
     * @return Mirror url.
     * @throws IOException If failed.
     */
    private String findMirror() throws IOException {
        String response = getHttpContents(new URL(APACHE_MIRROR_URL));

        if (response == null)
            throw new RuntimeException("Failed to retrieve mirrors");

        ObjectMapper mapper = new ObjectMapper();
        JsonNode mirrorUrl = mapper.readTree(response).get("preferred");

        if (mirrorUrl == null)
            throw new RuntimeException("Failed to find the preferred mirror");

        return mirrorUrl.asText();
    }

    /**
     * Attempts to obtain the latest version.
     *
     * @return Latest version.
     * @throws IOException If failed.
     */
    private String findLatestVersion() throws IOException {
        String response = getHttpContents(new URL(IGNITE_LATEST_VERSION_URL));

        if (response == null)
            throw new RuntimeException("Failed to identify the latest version. Specify it with " + IGNITE_VERSION);

        Matcher m = VERSION_PATTERN.matcher(response);
        if (m.find())
            return m.group();
        else
            throw new RuntimeException("Failed to retrieve the latest version. Specify it with " + IGNITE_VERSION);
    }

    /**
     * @param url Url.
     * @return Contents.
     * @throws IOException If failed.
     */
    private String getHttpContents(URL url) throws IOException {
        HttpURLConnection conn = (HttpURLConnection)url.openConnection();

        int code = conn.getResponseCode();

        if (code != 200)
            throw null;

        BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));
        return rd.lines().collect(Collectors.joining());
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

                log.log(Level.INFO, "Downloading from {0}", url.toString());

                FileOutputStream outFile = new FileOutputStream(Paths.get(downloadFolder, fileName).toFile());

                outFile.getChannel().transferFrom(Channels.newChannel(conn.getInputStream()), 0, Long.MAX_VALUE);

                outFile.close();

                return fileName;
            }
            else
                throw new RuntimeException("Got unexpected response code. Response code: " + code + " from " + url);
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
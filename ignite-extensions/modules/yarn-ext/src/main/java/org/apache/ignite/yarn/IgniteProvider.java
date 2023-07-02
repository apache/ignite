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
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.channels.Channels;
import java.util.Properties;
import java.util.logging.Logger;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.ignite.yarn.utils.IgniteYarnUtils;

/**
 * Downloads and stores Ignite release.
 */
public class IgniteProvider {
    /** */
    public static final Logger log = Logger.getLogger(IgniteProvider.class.getSimpleName());

    /** */
    public static final String DOWNLOAD_LINK = "https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=/ignite/";

    /** */
    private ClusterProperties props;

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
     * @return Path to current Ignite release.
     */
    public Path getIgnite() throws Exception {
        File folder = checkDownloadFolder();

        Properties verProps = new Properties();

        try (InputStream is = IgniteProvider.class.getClassLoader().getResourceAsStream("ignite.properties")) {
            verProps.load(is);
        }

        String curVer = verProps.getProperty("ignite.version");

        if (curVer == null || curVer.isEmpty())
            throw new IllegalStateException("Failed to determine Ignite version");

        log.info("Searching for Ignite release " + curVer);

        File release = findIgnite(folder, curVer);

        if (release == null) {
            Path releaseOnHdfs = findIgnite(fs, props.igniteReleasesDir(), curVer);

            if (releaseOnHdfs != null)
                return releaseOnHdfs;

            release = updateIgnite(curVer);
        }

        return IgniteYarnUtils.copyLocalToHdfs(fs, release.getAbsolutePath(),
            props.igniteReleasesDir() + File.separator + release.getName());
    }

    /**
     * @param folder Folder.
     * @param curVer Ignite version.
     * @return Ignite archives.
     */
    private File findIgnite(File folder, String curVer) {
        String[] files = folder.list();

        if (files != null) {
            for (String fileName : files) {
                if (fileName.equals(igniteRelease(curVer))) {
                    log.info("Found local release at " + folder.getAbsolutePath());

                    return new File(folder, fileName);
                }
            }
        }

        return null;
    }

    /**
     * @param fs File system,
     * @param folder Folder.
     * @param curVer Ignite version.
     * @return Ignite archives.
     */
    private Path findIgnite(FileSystem fs, String folder, String curVer) {
        FileStatus[] fileStatuses = null;

        try {
            fileStatuses = fs.listStatus(new Path(folder));
        }
        catch (FileNotFoundException ignored) {
            // Ignore. Folder doesn't exist.
        }
        catch (Exception e) {
            throw new RuntimeException("Couldn't get list files from hdfs.", e);
        }

        if (fileStatuses != null) {
            for (FileStatus file : fileStatuses) {
                String fileName = file.getPath().getName();

                if (fileName.equals(igniteRelease(curVer))) {
                    log.info("Found HDFS release at " + file.getPath());

                    return file.getPath();
                }
            }
        }

        return null;
    }

    /**
     * @param igniteUrl Download link.
     * @return Ignite.
     */
    public Path getIgnite(String igniteUrl) throws Exception {
        checkDownloadFolder();

        // Download ignite.
        String fileName = downloadIgnite(igniteUrl);

        Path dst = new Path(props.igniteReleasesDir() + File.separator + fileName);

        log.info("Using specified release at " + igniteUrl);

        if (!fs.exists(dst))
            fs.copyFromLocalFile(new Path(props.igniteLocalWorkDir() + File.separator + fileName), dst);

        return dst;
    }

    /**
     * @return File name.
     */
    private static String igniteRelease(String version) {
        return "apache-ignite-" + version + "-bin.zip";
    }

    /**
     * @param curVer Ignite version.
     * @return Current version if the current version is latest; new ignite version otherwise.
     */
    private File updateIgnite(String curVer) {
        try {
            // Such as https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=/ignite/2.7.0/apache-ignite-2.7.0-bin.zip
            URL url = new URL(DOWNLOAD_LINK + curVer + "/" + igniteRelease(curVer));

            HttpURLConnection conn = (HttpURLConnection)url.openConnection();

            int code = conn.getResponseCode();

            String redirectUrl = conn.getURL().toString();

            if (code == 301 || code == 302) {
                redirectUrl = conn.getHeaderField("Location");

                conn.disconnect();

                conn = (HttpURLConnection)new URL(redirectUrl).openConnection();
            }
            else if (code != 200)
                throw new RuntimeException("Got unexpected response code. Response code: " + code);

            checkDownloadFolder();

            File ignite = new File(props.igniteLocalWorkDir(), fileName(redirectUrl));

            FileOutputStream outFile = new FileOutputStream(ignite);

            outFile.getChannel().transferFrom(Channels.newChannel(conn.getInputStream()), 0, Long.MAX_VALUE);

            outFile.close();

            log.info("Found remote release at " + redirectUrl);

            return ignite;
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
     * @return File name.
     */
    private static String fileName(String url) {
        String[] split = url.split("/");

        return split[split.length - 1];
    }
}

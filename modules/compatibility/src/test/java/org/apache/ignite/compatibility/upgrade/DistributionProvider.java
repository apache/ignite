package org.apache.ignite.compatibility.upgrade;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Paths;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.SystemProperty;
import org.apache.ignite.internal.util.typedef.X;

/** */
public class DistributionProvider {
    /** Base Ignite distribution location */
    @SystemProperty(value = "Base Ignite distribution location", type = String.class)
    private static final String BASE_DIST = "ignite.upgrade.base";

    /** Target Ignite distribution location */
    @SystemProperty(value = "Target Ignite distribution location", type = String.class)
    private static final String TARGET_DIST = "ignite.upgrade.target";

    /** Cached Ignite released distributions location */
    @SystemProperty(value = "Cached Ignite released distributions location", type = String.class)
    private static final String RELEASES_DIR = "ignite.upgrade.releases";

    /** */
    private static final String DEFAULT_TARGET_REL_PATH = "target/release-package-apache-ignite";

    /** */
    private static final String LATEST_VERSION = "2.18.0";

    /** Resolves the Target distribution. */
    public static File resolveTargetDist() {
        String explicit = IgniteSystemProperties.getString(TARGET_DIST);

        if (explicit != null && !explicit.isEmpty())
            return new File(explicit);

        File cur = new File(System.getProperty("user.dir"));

        while (cur != null) {
            File potential = new File(cur, DEFAULT_TARGET_REL_PATH);

            if (potential.exists() && potential.isDirectory())
                return potential;

            cur = cur.getParentFile();
        }

        throw new IllegalStateException(
            "Target distribution not found. " +
            "Build with '-Prelease' or set -D" + TARGET_DIST
        );
    }

    /** Resolves the Base distribution. */
    public static File resolveBaseDist() {
        String explicit = IgniteSystemProperties.getString(BASE_DIST);

        if (explicit != null && !explicit.isEmpty())
            return new File(explicit);

        File cacheDir = getCacheDir();
        File versionedDir = new File(cacheDir, "ignite-" + LATEST_VERSION);

        if (!versionedDir.exists())
            downloadAndUnpack(LATEST_VERSION, versionedDir);

        return versionedDir;
    }

    /** */
    public static File getCacheDir() {
        String tmpDir = Paths.get(System.getProperty("user.dir"), "/target/upgrade-test/ignite-releases").toString();
        String path = IgniteSystemProperties.getString(RELEASES_DIR, tmpDir);

        assert path != null : "Ignite releases directory is null!";

        File dir = new File(path);

        if (!dir.exists())
            if (!dir.mkdirs())
                throw new IllegalStateException("Failed to create directory: " + dir.getAbsolutePath());

        return dir;
    }

    /** Downloads and unpacks Ignite distribution from Apache Archives. */
    private static void downloadAndUnpack(String ver, File dest) {
        String zipName = "apache-ignite-" + ver + "-bin.zip";
        String downloadUrl = "https://archive.apache.org/dist/ignite/" + ver + "/" + zipName;
        File tmpZip = new File(dest.getParentFile(), zipName);

        try {
            X.println("Downloading Ignite " + ver + " from " + downloadUrl + " to " + tmpZip.getAbsolutePath());

            try {
                downloadWithProgress(downloadUrl, tmpZip);
            } catch (IOException e) {
                X.println("Download failed: " + e.getMessage());
            }

            X.println("Unpacking to " + dest.getParentFile().getAbsolutePath());

            try (ZipInputStream zis = new ZipInputStream(new FileInputStream(tmpZip))) {
                ZipEntry entry;
                byte[] buf = new byte[8192];

                while ((entry = zis.getNextEntry()) != null) {
                    File file = new File(dest.getParentFile(), entry.getName());

                    if (entry.isDirectory())
                        file.mkdirs();
                    else {
                        file.getParentFile().mkdirs();

                        try (OutputStream out = new FileOutputStream(file)) {
                            int len;

                            while ((len = zis.read(buf)) > 0)
                                out.write(buf, 0, len);
                        }
                    }

                    zis.closeEntry();
                }
            }

            File extractedDir = new File(dest.getParentFile(), "apache-ignite-" + ver + "-bin");

            if (extractedDir.exists()) {
                if (!extractedDir.renameTo(dest))
                    throw new IOException("Failed to rename " + extractedDir + " to " + dest);
            }

            tmpZip.delete();

            X.println("Ignite " + ver + " is ready at " + dest.getAbsolutePath());
        }
        catch (IOException e) {
            if (tmpZip.exists())
                tmpZip.delete();

            throw new RuntimeException("Failed to fetch or unpack Ignite " + ver, e);
        }
    }

    /** */
    private static void downloadWithProgress(String downloadUrl, File targetFile) throws IOException {
        URL url = new URL(downloadUrl);
        HttpURLConnection connection = (HttpURLConnection)url.openConnection();

        long totalSize = connection.getContentLengthLong();

        try (InputStream in = connection.getInputStream();
             FileOutputStream out = new FileOutputStream(targetFile)) {

            byte[] buf = new byte[8192];
            int bytesRead;
            long totalRead = 0;
            int lastPercent = -1;

            while ((bytesRead = in.read(buf)) != -1) {
                out.write(buf, 0, bytesRead);
                totalRead += bytesRead;

                if (totalSize > 0) {
                    int percent = (int)(totalRead * 100 / totalSize);

                    if (percent != lastPercent) {
                        X.println("Progress: " + percent + "% [" +
                            (totalRead / 1024) + " KB / " + (totalSize / 1024) + " KB]");

                        lastPercent = percent;
                    }
                } else
                    X.println("Downloaded: " + (totalRead / 1024) + " KB (Unknown size)");
            }

            X.println("Download finished.");
        }
    }
}

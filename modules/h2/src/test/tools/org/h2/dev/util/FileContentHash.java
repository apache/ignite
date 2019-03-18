/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.h2.store.fs.FileUtils;
import org.h2.util.SortedProperties;
import org.h2.util.StringUtils;

/**
 * A utility to calculate the content hash of files. It should help detect
 * duplicate files and differences between directories.
 */
public class FileContentHash {

    // find empty directories:
    // find . -type d -empty
    // find . -name .hash.prop -delete

    private static final boolean WRITE_HASH_INDEX = true;
    private static final String HASH_INDEX = ".hash.prop";
    private static final int MIN_SIZE = 0;
    private final HashMap<String, String> hashes = new HashMap<>();
    private long nextLog;

    /**
     * Run the viewer.
     *
     * @param args the command line arguments
     */
    public static void main(String... args) throws IOException {
        new FileContentHash().runTool(args);
    }

    private void runTool(String... args) throws IOException {
        if (args.length == 0) {
            System.out.println("Usage: java " + getClass().getName() + " <dir>");
            return;
        }
        for (int i = 0; i < args.length; i++) {
            Info info = hash(args[i]);
            System.out.println("size: " + info.size);
        }
    }

    private static MessageDigest createMessageDigest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    private Info hash(String path) throws IOException {
        if (FileUtils.isDirectory(path)) {
            long totalSize = 0;
            SortedProperties propOld;
            SortedProperties propNew = new SortedProperties();
            String hashFileName = path + "/" + HASH_INDEX;
            if (FileUtils.exists(hashFileName)) {
                propOld = SortedProperties.loadProperties(hashFileName);
            } else {
                propOld = new SortedProperties();
            }
            List<String> list = FileUtils.newDirectoryStream(path);
            Collections.sort(list);
            MessageDigest mdDir = createMessageDigest();
            for (String f : list) {
                String name = FileUtils.getName(f);
                if (name.equals(HASH_INDEX)) {
                    continue;
                }
                long length = FileUtils.size(f);
                String entry = "name_" + name +
                        "-mod_" + FileUtils.lastModified(f) +
                        "-size_" + length;
                String hash = propOld.getProperty(entry);
                if (hash == null || FileUtils.isDirectory(f)) {
                    Info info = hash(f);
                    byte[] b = info.hash;
                    hash = StringUtils.convertBytesToHex(b);
                    totalSize += info.size;
                    entry = "name_" + name +
                            "-mod_" + FileUtils.lastModified(f) +
                            "-size_" + info.size;
                } else {
                    totalSize += length;
                    checkCollision(f, length, StringUtils.convertHexToBytes(hash));
                }
                propNew.put(entry, hash);
                mdDir.update(entry.getBytes(StandardCharsets.UTF_8));
                mdDir.update(hash.getBytes(StandardCharsets.UTF_8));
            }
            String oldFile = propOld.toString();
            String newFile = propNew.toString();
            if (!oldFile.equals(newFile)) {
                if (WRITE_HASH_INDEX) {
                    propNew.store(path + "/" + HASH_INDEX);
                }
            }
            Info info = new Info();
            info.hash = mdDir.digest();
            info.size = totalSize;
            return info;
        }
        MessageDigest md = createMessageDigest();
        InputStream in = FileUtils.newInputStream(path);
        long length = FileUtils.size(path);
        byte[] buff = new byte[1024 * 1024];
        while (true) {
            int len = in.read(buff);
            if (len < 0) {
                break;
            }
            md.update(buff, 0, len);
            long t = System.nanoTime();
            if (nextLog == 0 || t > nextLog) {
                System.out.println("Checking " + path);
                nextLog = t + 5000 * 1000000L;
            }
        }
        in.close();
        byte[] b = md.digest();
        checkCollision(path, length, b);
        Info info = new Info();
        info.hash = b;
        info.size = length;
        return info;
    }

    private void checkCollision(String path, long length, byte[] hash) {
        if (length < MIN_SIZE) {
            return;
        }
        String s = StringUtils.convertBytesToHex(hash);
        String old = hashes.get(s);
        if (old != null) {
            System.out.println("Collision: " + old + "\n" + path + "\n");
        } else {
            hashes.put(s,  path);
        }
    }

    /**
     * The info for a file.
     */
    static class Info {

        /**
         * The content hash.
         */
        byte[] hash;

        /**
         * The size in bytes.
         */
        long size;
    }

}

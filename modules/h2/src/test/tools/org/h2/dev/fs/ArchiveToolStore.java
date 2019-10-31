/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.fs;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.Random;
import org.h2.mvstore.Cursor;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.store.fs.FileUtils;
import org.h2.util.New;

/**
 * An archive tool to compress directories, using the MVStore backend.
 */
public class ArchiveToolStore {

    private static final int[] RANDOM = new int[256];
    private static final int MB = 1000 * 1000;
    private long lastTime;
    private long start;
    private int bucket;
    private String fileName;

    static {
        Random r = new Random(1);
        for (int i = 0; i < RANDOM.length; i++) {
            RANDOM[i] = r.nextInt();
        }
    }

    /**
     * Run the tool.
     *
     * @param args the command line arguments
     */
    public static void main(String... args) throws Exception {
        ArchiveToolStore app = new ArchiveToolStore();
        String arg = args.length != 3 ? null : args[0];
        if ("-compress".equals(arg)) {
            app.fileName = args[1];
            app.compress(args[2]);
        } else if ("-extract".equals(arg)) {
            app.fileName = args[1];
            app.expand(args[2]);
        } else {
            System.out.println("Command line options:");
            System.out.println("-compress <file> <sourceDir>");
            System.out.println("-extract <file> <targetDir>");
        }
    }

    private void compress(String sourceDir) throws Exception {
        start();
        long tempSize = 8 * 1024 * 1024;
        String tempFileName = fileName + ".temp";
        ArrayList<String> fileNames = New.arrayList();

        System.out.println("Reading the file list");
        long totalSize = addFiles(sourceDir, fileNames);
        System.out.println("Compressing " + totalSize / MB + " MB");

        FileUtils.delete(tempFileName);
        FileUtils.delete(fileName);
        MVStore storeTemp = new MVStore.Builder().
                fileName(tempFileName).
                autoCommitDisabled().
                open();
        final MVStore store = new MVStore.Builder().
                fileName(fileName).
                pageSplitSize(2 * 1024 * 1024).
                compressHigh().
                autoCommitDisabled().
                open();
        MVMap<String, int[]> filesTemp = storeTemp.openMap("files");
        long currentSize = 0;
        int segmentId = 1;
        int segmentLength = 0;
        ByteBuffer buff = ByteBuffer.allocate(1024 * 1024);
        for (String s : fileNames) {
            String name = s.substring(sourceDir.length() + 1);
            if (FileUtils.isDirectory(s)) {
                // directory
                filesTemp.put(name, new int[1]);
                continue;
            }
            buff.clear();
            buff.flip();
            ArrayList<Integer> posList = new ArrayList<>();
            try (FileChannel fc = FileUtils.open(s, "r")) {
                boolean eof = false;
                while (true) {
                    while (!eof && buff.remaining() < 512 * 1024) {
                        int remaining = buff.remaining();
                        buff.compact();
                        buff.position(remaining);
                        int l = fc.read(buff);
                        if (l < 0) {
                            eof = true;
                        }
                        buff.flip();
                    }
                    if (buff.remaining() == 0) {
                        break;
                    }
                    int position = buff.position();
                    int c = getChunkLength(buff.array(), position,
                            buff.limit()) - position;
                    byte[] bytes = Arrays.copyOfRange(buff.array(), position, position + c);
                    buff.position(position + c);
                    int[] key = getKey(bucket, bytes);
                    key[3] = segmentId;
                    while (true) {
                        MVMap<int[], byte[]> data = storeTemp.
                                openMap("data" + segmentId);
                        byte[] old = data.get(key);
                        if (old == null) {
                            // new
                            data.put(key, bytes);
                            break;
                        }
                        if (Arrays.equals(old, bytes)) {
                            // duplicate
                            break;
                        }
                        // same checksum: change checksum
                        key[2]++;
                    }
                    for (int i = 0; i < key.length; i++) {
                        posList.add(key[i]);
                    }
                    segmentLength += c;
                    currentSize += c;
                    if (segmentLength > tempSize) {
                        storeTemp.commit();
                        segmentId++;
                        segmentLength = 0;
                    }
                    printProgress(0, 50, currentSize, totalSize);
                }
            }
            int[] posArray = new int[posList.size()];
            for (int i = 0; i < posList.size(); i++) {
                posArray[i] = posList.get(i);
            }
            filesTemp.put(name, posArray);
        }
        storeTemp.commit();
        ArrayList<Cursor<int[], byte[]>> list = New.arrayList();
        totalSize = 0;
        for (int i = 1; i <= segmentId; i++) {
            MVMap<int[], byte[]> data = storeTemp.openMap("data" + i);
            totalSize += data.sizeAsLong();
            Cursor<int[], byte[]> c = data.cursor(null);
            if (c.hasNext()) {
                c.next();
                list.add(c);
            }
        }
        segmentId = 1;
        segmentLength = 0;
        currentSize = 0;
        MVMap<int[], byte[]> data = store.openMap("data" + segmentId);
        MVMap<int[], Boolean> keepSegment = storeTemp.openMap("keep");
        while (list.size() > 0) {
            Collections.sort(list, new Comparator<Cursor<int[], byte[]>>() {

                @Override
                public int compare(Cursor<int[], byte[]> o1,
                        Cursor<int[], byte[]> o2) {
                    int[] k1 = o1.getKey();
                    int[] k2 = o2.getKey();
                    int comp = 0;
                    for (int i = 0; i < k1.length - 1; i++) {
                        long x1 = k1[i];
                        long x2 = k2[i];
                        if (x1 > x2) {
                            comp = 1;
                            break;
                        } else if (x1 < x2) {
                            comp = -1;
                            break;
                        }
                    }
                    return comp;
                }

            });
            Cursor<int[], byte[]> top = list.get(0);
            int[] key = top.getKey();
            byte[] bytes = top.getValue();
            int[] k2 = Arrays.copyOf(key, key.length);
            k2[key.length - 1] = 0;
            // TODO this lookup can be avoided
            // if we remember the last entry with k[..] = 0
            byte[] old = data.get(k2);
            if (old == null) {
                if (segmentLength > tempSize) {
                    // switch only for new entries
                    // where segmentId is 0,
                    // so that entries with the same
                    // key but different segmentId
                    // are in the same segment
                    store.commit();
                    segmentLength = 0;
                    segmentId++;
                    data = store.openMap("data" + segmentId);
                }
                key = k2;
                // new entry
                data.put(key, bytes);
                segmentLength += bytes.length;
            } else if (Arrays.equals(old, bytes)) {
                // duplicate
            } else {
                // almost a duplicate:
                // keep segment id
                keepSegment.put(key, Boolean.TRUE);
                data.put(key, bytes);
                segmentLength += bytes.length;
            }
            if (!top.hasNext()) {
                list.remove(0);
            } else {
                top.next();
            }
            currentSize++;
            printProgress(50, 100, currentSize, totalSize);
        }
        MVMap<String, int[]> files = store.openMap("files");
        for (Entry<String, int[]> e : filesTemp.entrySet()) {
            String k = e.getKey();
            int[] ids = e.getValue();
            if (ids.length == 1) {
                // directory
                files.put(k, ids);
                continue;
            }
            int[] newIds = Arrays.copyOf(ids, ids.length);
            for (int i = 0; i < ids.length; i += 4) {
                int[] id = new int[4];
                id[0] = ids[i];
                id[1] = ids[i + 1];
                id[2] = ids[i + 2];
                id[3] = ids[i + 3];
                if (!keepSegment.containsKey(id)) {
                    newIds[i + 3] = 0;
                }
            }
            files.put(k, newIds);
        }
        store.commit();
        storeTemp.close();
        FileUtils.delete(tempFileName);
        store.close();
        System.out.println();
        System.out.println("Compressed to " +
                FileUtils.size(fileName) / MB + " MB");
        printDone();
    }

    private void start() {
        this.start = System.nanoTime();
        this.lastTime = start;
    }

    private void printProgress(int low, int high, long current, long total) {
        long now = System.nanoTime();
        if (now - lastTime > TimeUnit.SECONDS.toNanos(5)) {
            System.out.print((low + (high - low) * current / total) + "% ");
            lastTime = now;
        }
    }

    private void printDone() {
        System.out.println("Done in " +
                TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start) +
                " seconds");
    }

    private static long addFiles(String dir, ArrayList<String> list) {
        long size = 0;
        for (String s : FileUtils.newDirectoryStream(dir)) {
            if (FileUtils.isDirectory(s)) {
                size += addFiles(s, list);
            } else {
                size += FileUtils.size(s);
            }
            list.add(s);
        }
        return size;
    }

    private void expand(String targetDir) throws Exception {
        start();
        long tempSize = 8 * 1024 * 1024;
        String tempFileName = fileName + ".temp";
        FileUtils.createDirectories(targetDir);
        MVStore store = new MVStore.Builder().
                fileName(fileName).open();
        MVMap<String, int[]> files = store.openMap("files");
        System.out.println("Extracting " + files.size() + " files");
        MVStore storeTemp = null;
        FileUtils.delete(tempFileName);
        long totalSize = 0;
        int lastSegment = 0;
        for (int i = 1;; i++) {
            if (!store.hasMap("data" + i)) {
                lastSegment = i - 1;
                break;
            }
        }

        storeTemp = new MVStore.Builder().
                fileName(tempFileName).
                autoCommitDisabled().
                open();

        MVMap<Integer, String> fileNames = storeTemp.openMap("fileNames");

        MVMap<String, int[]> filesTemp = storeTemp.openMap("files");
        int fileId = 0;
        for (Entry<String, int[]> e : files.entrySet()) {
            fileNames.put(fileId++, e.getKey());
            filesTemp.put(e.getKey(), e.getValue());
            totalSize += e.getValue().length / 4;
        }
        storeTemp.commit();

        files = filesTemp;
        long currentSize = 0;
        int chunkSize = 0;
        for (int s = 1; s <= lastSegment; s++) {
            MVMap<int[], byte[]> segmentData = store.openMap("data" + s);
            // key: fileId, blockId; value: data
            MVMap<int[], byte[]> fileData = storeTemp.openMap("fileData" + s);
            fileId = 0;
            for (Entry<String, int[]> e : files.entrySet()) {
                int[] keys = e.getValue();
                if (keys.length == 1) {
                    fileId++;
                    continue;
                }
                for (int i = 0; i < keys.length; i += 4) {
                    int[] dk = new int[4];
                    dk[0] = keys[i];
                    dk[1] = keys[i + 1];
                    dk[2] = keys[i + 2];
                    dk[3] = keys[i + 3];
                    byte[] bytes = segmentData.get(dk);
                    if (bytes != null) {
                        int[] k = new int[] { fileId, i / 4 };
                        fileData.put(k, bytes);
                        chunkSize += bytes.length;
                        if (chunkSize > tempSize) {
                            storeTemp.commit();
                            chunkSize = 0;
                        }
                        currentSize++;
                        printProgress(0, 50, currentSize, totalSize);
                    }
                }
                fileId++;
            }
            storeTemp.commit();
        }

        ArrayList<Cursor<int[], byte[]>> list = New.arrayList();
        totalSize = 0;
        currentSize = 0;
        for (int i = 1; i <= lastSegment; i++) {
            MVMap<int[], byte[]> fileData = storeTemp.openMap("fileData" + i);
            totalSize += fileData.sizeAsLong();
            Cursor<int[], byte[]> c = fileData.cursor(null);
            if (c.hasNext()) {
                c.next();
                list.add(c);
            }
        }
        String lastFileName = null;
        OutputStream file = null;
        int[] lastKey = null;
        while (list.size() > 0) {
            Collections.sort(list, new Comparator<Cursor<int[], byte[]>>() {

                @Override
                public int compare(Cursor<int[], byte[]> o1,
                        Cursor<int[], byte[]> o2) {
                    int[] k1 = o1.getKey();
                    int[] k2 = o2.getKey();
                    int comp = 0;
                    for (int i = 0; i < k1.length; i++) {
                        long x1 = k1[i];
                        long x2 = k2[i];
                        if (x1 > x2) {
                            comp = 1;
                            break;
                        } else if (x1 < x2) {
                            comp = -1;
                            break;
                        }
                    }
                    return comp;
                }

            });
            Cursor<int[], byte[]> top = list.get(0);
            int[] key = top.getKey();
            byte[] bytes = top.getValue();
            String f = targetDir + "/" + fileNames.get(key[0]);
            if (!f.equals(lastFileName)) {
                if (file != null) {
                    file.close();
                }
                String p = FileUtils.getParent(f);
                if (p != null) {
                    FileUtils.createDirectories(p);
                }
                file = new BufferedOutputStream(new FileOutputStream(f));
                lastFileName = f;
            } else {
                if (key[0] != lastKey[0] || key[1] != lastKey[1] + 1) {
                    System.out.println("missing entry after " + Arrays.toString(lastKey));
                }
            }
            lastKey = key;
            file.write(bytes);
            if (!top.hasNext()) {
                list.remove(0);
            } else {
                top.next();
            }
            currentSize++;
            printProgress(50, 100, currentSize, totalSize);
        }
        for (Entry<String, int[]> e : files.entrySet()) {
            String f = targetDir + "/" + e.getKey();
            int[] keys = e.getValue();
            if (keys.length == 1) {
                FileUtils.createDirectories(f);
            } else if (keys.length == 0) {
                // empty file
                String p = FileUtils.getParent(f);
                if (p != null) {
                    FileUtils.createDirectories(p);
                }
                new FileOutputStream(f).close();
            }
        }
        if (file != null) {
            file.close();
        }
        store.close();

        storeTemp.close();
        FileUtils.delete(tempFileName);

        System.out.println();
        printDone();
    }

    private int getChunkLength(byte[] data, int start, int maxPos) {
        int minLen = 4 * 1024;
        int mask = 4 * 1024 - 1;
        int factor = 31;
        int hash = 0, mul = 1, offset = 8;
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        int i = start;
        int[] rand = RANDOM;
        for (int j = 0; i < maxPos; i++, j++) {
            hash = hash * factor + rand[data[i] & 255];
            if (j >= offset) {
                hash -= mul * rand[data[i - offset] & 255];
            } else {
                mul *= factor;
            }
            if (hash < min) {
                min = hash;
            }
            if (hash > max) {
                max = hash;
            }
            if (j > minLen) {
                if (j > minLen * 4) {
                    break;
                }
                if ((hash & mask) == 1) {
                    break;
                }
            }
        }
        bucket = min;
        return i;
    }

    private static int[] getKey(int bucket, byte[] buff) {
        int[] key = new int[4];
        int[] counts = new int[8];
        int len = buff.length;
        for (int i = 0; i < len; i++) {
            int x = buff[i] & 0xff;
            counts[x >> 5]++;
        }
        int cs = 0;
        for (int i = 0; i < 8; i++) {
            cs *= 2;
            if (counts[i] > (len / 32)) {
                cs += 1;
            }
        }
        key[0] = cs;
        key[1] = bucket;
        key[2] = DataUtils.getFletcher32(buff, 0, buff.length);
        return key;
    }

}

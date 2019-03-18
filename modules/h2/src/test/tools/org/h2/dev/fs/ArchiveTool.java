/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.fs;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

/**
 * A standalone archive tool to compress directories. It does not have any
 * dependencies except for the Java libraries.
 * <p>
 * Unlike other compression tools, it splits the data into chunks and sorts the
 * chunks, so that large directories or files that contain duplicate data are
 * compressed much better.
 */
public class ArchiveTool {

    /**
     * The file header.
     */
    private static final byte[] HEADER = {'H', '2', 'A', '1'};

    /**
     * The number of bytes per megabyte (used for the output).
     */
    private static final int MB = 1000 * 1000;

    /**
     * Run the tool.
     *
     * @param args the command line arguments
     */
    public static void main(String... args) throws Exception {
        Log log = new Log();
        int level = Integer.getInteger("level", Deflater.BEST_SPEED);
        if (args.length == 1) {
            File f = new File(args[0]);
            if (f.exists()) {
                if (f.isDirectory()) {
                    String fromDir = f.getAbsolutePath();
                    String toFile = fromDir + ".at";
                    compress(fromDir, toFile, level);
                    return;
                }
                String fromFile = f.getAbsolutePath();
                int dot = fromFile.lastIndexOf('.');
                if (dot > 0 && dot > fromFile.replace('\\', '/').lastIndexOf('/')) {
                    String toDir = fromFile.substring(0, dot);
                    extract(fromFile, toDir);
                    return;
                }
            }
        }
        String arg = args.length != 3 ? null : args[0];
        if ("-compress".equals(arg)) {
            String toFile = args[1];
            String fromDir = args[2];
            compress(fromDir, toFile, level);
        } else if ("-extract".equals(arg)) {
            String fromFile = args[1];
            String toDir = args[2];
            extract(fromFile, toDir);
        } else {
            log.println("An archive tool to efficiently compress large directories");
            log.println("Command line options:");
            log.println("<sourceDir>");
            log.println("<compressedFile>");
            log.println("-compress <compressedFile> <sourceDir>");
            log.println("-extract <compressedFile> <targetDir>");
        }
    }

    private static void compress(String fromDir, String toFile, int level) throws IOException {
        final Log log = new Log();
        final long start = System.nanoTime();
        final long startMs = System.currentTimeMillis();
        final AtomicBoolean title = new AtomicBoolean();
        long size = getSize(new File(fromDir), new Runnable() {
            int count;
            long lastTime = start;
            @Override
            public void run() {
                count++;
                if (count % 1000 == 0) {
                    long now = System.nanoTime();
                    if (now - lastTime > TimeUnit.SECONDS.toNanos(3)) {
                        if (!title.getAndSet(true)) {
                            log.println("Counting files");
                        }
                        log.print(count + " ");
                        lastTime = now;
                    }
                }
            }
        });
        if (title.get()) {
            log.println();
        }
        log.println("Compressing " + size / MB + " MB at " +
                new java.sql.Time(startMs).toString());
        InputStream in = getDirectoryInputStream(fromDir);
        String temp = toFile + ".temp";
        OutputStream out =
                new BufferedOutputStream(
                                new FileOutputStream(toFile), 1024 * 1024);
        Deflater def = new Deflater();
        def.setLevel(level);
        out = new BufferedOutputStream(
                new DeflaterOutputStream(out, def), 1024 * 1024);
        sort(log, in, out, temp, size);
        in.close();
        out.close();
        log.println();
        log.println("Compressed to " +
                new File(toFile).length() / MB + " MB in " +
                TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start)  +
                " seconds");
        log.println();
    }

    private static void extract(String fromFile, String toDir) throws IOException {
        Log log = new Log();
        long start = System.nanoTime();
        long startMs = System.currentTimeMillis();
        long size = new File(fromFile).length();
        log.println("Extracting " + size / MB + " MB at " + new java.sql.Time(startMs).toString());
        InputStream in =
                new BufferedInputStream(
                        new FileInputStream(fromFile), 1024 * 1024);
        String temp = fromFile + ".temp";
        Inflater inflater = new Inflater();
        in = new InflaterInputStream(in, inflater, 1024 * 1024);
        OutputStream out = getDirectoryOutputStream(toDir);
        combine(log, in, out, temp);
        inflater.end();
        in.close();
        out.close();
        log.println();
        log.println("Extracted in " +
                TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start) +
                " seconds");
    }

    private static long getSize(File f, Runnable r) {
        // assume a metadata entry is 40 bytes
        long size = 40;
        if (f.isDirectory()) {
            File[] list = f.listFiles();
            if (list != null) {
                for (File c : list) {
                    size += getSize(c, r);
                }
            }
        } else {
            size += f.length();
        }
        r.run();
        return size;
    }

    private static InputStream getDirectoryInputStream(final String dir) {

        File f = new File(dir);
        if (!f.isDirectory() || !f.exists()) {
            throw new IllegalArgumentException("Not an existing directory: " + dir);
        }

        return new InputStream() {

            private final String baseDir;
            private final ArrayList<String> files = new ArrayList<>();
            private String current;
            private ByteArrayInputStream meta;
            private DataInputStream fileIn;
            private long remaining;

            {
                File f = new File(dir);
                baseDir = f.getAbsolutePath();
                addDirectory(f);
            }

            private void addDirectory(File f) {
                File[] list = f.listFiles();
                if (list != null) {
                    // first all directories, then all files
                    for (File c : list) {
                        if (c.isDirectory()) {
                            files.add(c.getAbsolutePath());
                        }
                    }
                    for (File c : list) {
                        if (c.isFile()) {
                            files.add(c.getAbsolutePath());
                        }
                    }
                }
            }

            // int: metadata length
            // byte: 0: directory, 1: file
            // varLong: lastModified
            // byte: 0: read-write, 1: read-only
            // (file only) varLong: file length
            // utf-8: file name

            @Override
            public int read() throws IOException {
                if (meta != null) {
                    // read from the metadata
                    int x = meta.read();
                    if (x >= 0) {
                        return x;
                    }
                    meta = null;
                }
                if (fileIn != null) {
                    if (remaining > 0) {
                        // read from the file
                        int x = fileIn.read();
                        remaining--;
                        if (x < 0) {
                            throw new EOFException();
                        }
                        return x;
                    }
                    fileIn.close();
                    fileIn = null;
                }
                if (files.size() == 0) {
                    // EOF
                    return -1;
                }
                // breadth-first traversal
                // first all files, then all directories
                current = files.remove(files.size() - 1);
                File f = new File(current);
                if (f.isDirectory()) {
                    addDirectory(f);
                }
                ByteArrayOutputStream metaOut = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(metaOut);
                boolean isFile = f.isFile();
                out.writeInt(0);
                out.write(isFile ? 1 : 0);
                out.write(!f.canWrite() ? 1 : 0);
                writeVarLong(out, f.lastModified());
                if (isFile) {
                    remaining = f.length();
                    writeVarLong(out, remaining);
                    fileIn = new DataInputStream(new BufferedInputStream(
                            new FileInputStream(current), 1024 * 1024));
                }
                if (!current.startsWith(baseDir)) {
                    throw new IOException("File " + current + " does not start with " + baseDir);
                }
                String n = current.substring(baseDir.length() + 1);
                out.writeUTF(n);
                out.writeInt(metaOut.size());
                out.flush();
                byte[] bytes = metaOut.toByteArray();
                // copy metadata length to beginning
                System.arraycopy(bytes, bytes.length - 4, bytes, 0, 4);
                // cut the length
                bytes = Arrays.copyOf(bytes, bytes.length - 4);
                meta = new ByteArrayInputStream(bytes);
                return meta.read();
            }

            @Override
            public int read(byte[] buff, int offset, int length) throws IOException {
                if (meta != null || fileIn == null || remaining == 0) {
                    return super.read(buff, offset, length);
                }
                int l = (int) Math.min(length, remaining);
                fileIn.readFully(buff, offset, l);
                remaining -= l;
                return l;
            }

        };

    }

    private static OutputStream getDirectoryOutputStream(final String dir) {
        new File(dir).mkdirs();
        return new OutputStream() {

            private ByteArrayOutputStream meta = new ByteArrayOutputStream();
            private OutputStream fileOut;
            private File file;
            private long remaining = 4;
            private long modified;
            private boolean readOnly;

            @Override
            public void write(byte[] buff, int offset, int length) throws IOException {
                while (length > 0) {
                    if (fileOut == null || remaining <= 1) {
                        write(buff[offset] & 255);
                        offset++;
                        length--;
                    } else {
                        int l = (int) Math.min(length, remaining - 1);
                        fileOut.write(buff, offset, l);
                        remaining -= l;
                        offset += l;
                        length -= l;
                    }
                }
            }

            @Override
            public void write(int b) throws IOException {
                if (fileOut != null) {
                    fileOut.write(b);
                    if (--remaining > 0) {
                        return;
                    }
                    // this can be slow, but I don't know a way to avoid it
                    fileOut.close();
                    fileOut = null;
                    file.setLastModified(modified);
                    if (readOnly) {
                        file.setReadOnly();
                    }
                    remaining = 4;
                    return;
                }
                meta.write(b);
                if (--remaining > 0) {
                    return;
                }
                DataInputStream in = new DataInputStream(
                        new ByteArrayInputStream(meta.toByteArray()));
                if (meta.size() == 4) {
                    // metadata is next
                    remaining = in.readInt() - 4;
                    if (remaining > 16 * 1024) {
                        throw new IOException("Illegal directory stream");
                    }
                    return;
                }
                // read and ignore the length
                in.readInt();
                boolean isFile = in.read() == 1;
                readOnly = in.read() == 1;
                modified = readVarLong(in);
                if (isFile) {
                    remaining = readVarLong(in);
                } else {
                    remaining = 4;
                }
                String name = dir + "/" + in.readUTF();
                file = new File(name);
                if (isFile) {
                    if (remaining == 0) {
                        new File(name).createNewFile();
                        remaining = 4;
                    } else {
                        fileOut = new BufferedOutputStream(
                                new FileOutputStream(name), 1024 * 1024);
                    }
                } else {
                    file.mkdirs();
                    file.setLastModified(modified);
                    if (readOnly) {
                        file.setReadOnly();
                    }
                }
                meta.reset();
            }
        };
    }

    private static void sort(Log log, InputStream in, OutputStream out,
            String tempFileName, long size) throws IOException {
        int bufferSize = 32 * 1024 * 1024;
        DataOutputStream tempOut = new DataOutputStream(new BufferedOutputStream(
                new FileOutputStream(tempFileName), 1024 * 1024));
        byte[] bytes = new byte[bufferSize];
        List<Long> segmentStart = new ArrayList<>();
        long outPos = 0;
        long id = 1;

        // Temp file: segment* 0
        // Segment: chunk* 0
        // Chunk: pos* 0 sortKey data

        log.setRange(0, 30, size);
        while (true) {
            int len = readFully(in, bytes, bytes.length);
            if (len == 0) {
                break;
            }
            log.printProgress(len);
            TreeMap<Chunk, Chunk> map = new TreeMap<>();
            for (int pos = 0; pos < len;) {
                int[] key = getKey(bytes, pos, len);
                int l = key[3];
                byte[] buff = Arrays.copyOfRange(bytes, pos, pos + l);
                pos += l;
                Chunk c = new Chunk(null, key, buff);
                Chunk old = map.get(c);
                if (old == null) {
                    // new entry
                    c.idList = new ArrayList<>();
                    c.idList.add(id);
                    map.put(c, c);
                } else {
                    old.idList.add(id);
                }
                id++;
            }
            segmentStart.add(outPos);
            for (Chunk c : map.keySet()) {
                outPos += c.write(tempOut, true);
            }
            // end of segment
            outPos += writeVarLong(tempOut, 0);
        }
        tempOut.close();
        long tempSize = new File(tempFileName).length();

        // merge blocks if needed
        int blockSize = 64;
        boolean merge = false;
        while (segmentStart.size() > blockSize) {
            merge = true;
            log.setRange(30, 50, tempSize);
            log.println();
            log.println("Merging " + segmentStart.size() + " segments " + blockSize + ":1");
            ArrayList<Long> segmentStart2 = new ArrayList<>();
            outPos = 0;
            DataOutputStream tempOut2 = new DataOutputStream(new BufferedOutputStream(
                    new FileOutputStream(tempFileName + ".b"), 1024 * 1024));
            while (segmentStart.size() > 0) {
                segmentStart2.add(outPos);
                int s = Math.min(segmentStart.size(), blockSize);
                List<Long> start = segmentStart.subList(0, s);
                TreeSet<ChunkStream> segmentIn = new TreeSet<>();
                long read = openSegments(start, segmentIn, tempFileName, true);
                log.printProgress(read);
                Chunk last = null;
                Iterator<Chunk> it = merge(segmentIn, log);
                while (it.hasNext()) {
                    Chunk c = it.next();
                    if (last == null) {
                        last = c;
                    } else if (last.compareTo(c) == 0) {
                        for (long x : c.idList) {
                            last.idList.add(x);
                        }
                    } else {
                        outPos += last.write(tempOut2, true);
                        last = c;
                    }
                }
                if (last != null) {
                    outPos += last.write(tempOut2, true);
                }
                // end of segment
                outPos += writeVarLong(tempOut2, 0);
                segmentStart = segmentStart.subList(s, segmentStart.size());
            }
            segmentStart = segmentStart2;
            tempOut2.close();
            tempSize = new File(tempFileName).length();
            new File(tempFileName).delete();
            tempFileName += ".b";
        }
        if (merge) {
            log.println();
            log.println("Combining " + segmentStart.size() + " segments");
        }

        TreeSet<ChunkStream> segmentIn = new TreeSet<>();
        long read = openSegments(segmentStart, segmentIn, tempFileName, true);
        log.printProgress(read);

        DataOutputStream dataOut = new DataOutputStream(out);
        dataOut.write(HEADER);
        writeVarLong(dataOut, size);

        // File: header length chunk* 0
        // chunk: pos* 0 data
        log.setRange(50, 100, tempSize);
        Chunk last = null;
        Iterator<Chunk> it = merge(segmentIn, log);
        while (it.hasNext()) {
            Chunk c = it.next();
            if (last == null) {
                last = c;
            } else if (last.compareTo(c) == 0) {
                for (long x : c.idList) {
                    last.idList.add(x);
                }
            } else {
                last.write(dataOut, false);
                last = c;
            }
        }
        if (last != null) {
            last.write(dataOut, false);
        }
        new File(tempFileName).delete();
        writeVarLong(dataOut, 0);
        dataOut.flush();
    }

    private static long openSegments(List<Long> segmentStart, TreeSet<ChunkStream> segmentIn,
            String tempFileName, boolean readKey) throws IOException {
        long inPos = 0;
        int bufferTotal = 64 * 1024 * 1024;
        int bufferPerStream = bufferTotal / segmentStart.size();
        // FileChannel fc = new RandomAccessFile(tempFileName, "r").
        //     getChannel();
        for (int i = 0; i < segmentStart.size(); i++) {
            // long end = i < segmentStart.size() - 1 ?
            //     segmentStart.get(i+1) : fc.size();
            // InputStream in =
            //     new SharedInputStream(fc, segmentStart.get(i), end);
            InputStream in = new FileInputStream(tempFileName);
            in.skip(segmentStart.get(i));
            ChunkStream s = new ChunkStream(i);
            s.readKey = readKey;
            s.in = new DataInputStream(new BufferedInputStream(in, bufferPerStream));
            inPos += s.readNext();
            if (s.current != null) {
                segmentIn.add(s);
            }
        }
        return inPos;
    }

    private static Iterator<Chunk> merge(final TreeSet<ChunkStream> segmentIn, final Log log) {
        return new Iterator<Chunk>() {

            @Override
            public boolean hasNext() {
                return segmentIn.size() > 0;
            }

            @Override
            public Chunk next() {
                ChunkStream s = segmentIn.first();
                segmentIn.remove(s);
                Chunk c = s.current;
                int len = s.readNext();
                log.printProgress(len);
                if (s.current != null) {
                    segmentIn.add(s);
                }
                return c;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

        };
    }

    /**
     * Read a number of bytes. This method repeats reading until
     * either the bytes have been read, or EOF.
     *
     * @param in the input stream
     * @param buffer the target buffer
     * @param max the number of bytes to read
     * @return the number of bytes read (max unless EOF has been reached)
     */
    private static int readFully(InputStream in, byte[] buffer, int max)
            throws IOException {
        int result = 0, len = Math.min(max, buffer.length);
        while (len > 0) {
            int l = in.read(buffer, result, len);
            if (l < 0) {
                break;
            }
            result += l;
            len -= l;
        }
        return result;
    }

    /**
     * Get the sort key and length of a chunk.
     */
    private static int[] getKey(byte[] data, int start, int maxPos) {
        int minLen = 4 * 1024;
        int mask = 4 * 1024 - 1;
        long min = Long.MAX_VALUE;
        int pos = start;
        for (int j = 0; pos < maxPos; pos++, j++) {
            if (pos <= start + 10) {
                continue;
            }
            long hash = getSipHash24(data, pos - 10, pos, 111, 11224);
            if (hash < min) {
                min = hash;
            }
            if (j > minLen) {
                if ((hash & mask) == 1) {
                    break;
                }
                if (j > minLen * 4 && (hash & (mask >> 1)) == 1) {
                    break;
                }
                if (j > minLen * 16) {
                    break;
                }
            }
        }
        int len = pos - start;
        int[] counts = new int[8];
        for (int i = start; i < pos; i++) {
            int x = data[i] & 0xff;
            counts[x >> 5]++;
        }
        int cs = 0;
        for (int i = 0; i < 8; i++) {
            cs *= 2;
            if (counts[i] > (len / 32)) {
                cs += 1;
            }
        }
        int[] key = new int[4];
        // TODO test if cs makes a difference
        key[0] = (int) (min >>> 32);
        key[1] = (int) min;
        key[2] = cs;
        key[3] = len;
        return key;
    }

    private static long getSipHash24(byte[] b, int start, int end, long k0,
            long k1) {
        long v0 = k0 ^ 0x736f6d6570736575L;
        long v1 = k1 ^ 0x646f72616e646f6dL;
        long v2 = k0 ^ 0x6c7967656e657261L;
        long v3 = k1 ^ 0x7465646279746573L;
        int repeat;
        for (int off = start; off <= end + 8; off += 8) {
            long m;
            if (off <= end) {
                m = 0;
                int i = 0;
                for (; i < 8 && off + i < end; i++) {
                    m |= ((long) b[off + i] & 255) << (8 * i);
                }
                if (i < 8) {
                    m |= ((long) end - start) << 56;
                }
                v3 ^= m;
                repeat = 2;
            } else {
                m = 0;
                v2 ^= 0xff;
                repeat = 4;
            }
            for (int i = 0; i < repeat; i++) {
                v0 += v1;
                v2 += v3;
                v1 = Long.rotateLeft(v1, 13);
                v3 = Long.rotateLeft(v3, 16);
                v1 ^= v0;
                v3 ^= v2;
                v0 = Long.rotateLeft(v0, 32);
                v2 += v1;
                v0 += v3;
                v1 = Long.rotateLeft(v1, 17);
                v3 = Long.rotateLeft(v3, 21);
                v1 ^= v2;
                v3 ^= v0;
                v2 = Long.rotateLeft(v2, 32);
            }
            v0 ^= m;
        }
        return v0 ^ v1 ^ v2 ^ v3;
    }

    /**
     * Get the sort key and length of a chunk.
     */
    private static int[] getKeyOld(byte[] data, int start, int maxPos) {
        int minLen = 4 * 1024;
        int mask = 4 * 1024 - 1;
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        int pos = start;
        long bytes = 0;
        for (int j = 0; pos < maxPos; pos++, j++) {
            bytes = (bytes << 8) | (data[pos] & 255);
            int hash = getHash(bytes);
            if (hash < min) {
                min = hash;
            }
            if (hash > max) {
                max = hash;
            }
            if (j > minLen) {
                if ((hash & mask) == 1) {
                    break;
                }
                if (j > minLen * 4 && (hash & (mask >> 1)) == 1) {
                    break;
                }
                if (j > minLen * 16) {
                    break;
                }
            }
        }
        int len = pos - start;
        int[] counts = new int[8];
        for (int i = start; i < pos; i++) {
            int x = data[i] & 0xff;
            counts[x >> 5]++;
        }
        int cs = 0;
        for (int i = 0; i < 8; i++) {
            cs *= 2;
            if (counts[i] > (len / 32)) {
                cs += 1;
            }
        }
        int[] key = new int[4];
        key[0] = cs;
        key[1] = min;
        key[2] = max;
        key[3] = len;
        return key;
    }

    private static int getHash(long key) {
        int hash = (int) ((key >>> 32) ^ key);
        hash = ((hash >>> 16) ^ hash) * 0x45d9f3b;
        hash = ((hash >>> 16) ^ hash) * 0x45d9f3b;
        hash = (hash >>> 16) ^ hash;
        return hash;
    }

    private static void combine(Log log, InputStream in, OutputStream out,
            String tempFileName) throws IOException {
        int bufferSize = 16 * 1024 * 1024;
        DataOutputStream tempOut =
                new DataOutputStream(
                        new BufferedOutputStream(
                                new FileOutputStream(tempFileName), 1024 * 1024));

        // File: header length chunk* 0
        // chunk: pos* 0 data

        DataInputStream dataIn = new DataInputStream(in);
        byte[] header = new byte[4];
        dataIn.readFully(header);
        if (!Arrays.equals(header, HEADER)) {
            tempOut.close();
            throw new IOException("Invalid header");
        }
        long size = readVarLong(dataIn);
        long outPos = 0;
        List<Long> segmentStart = new ArrayList<>();
        boolean end = false;

        // Temp file: segment* 0
        // Segment: chunk* 0
        // Chunk: pos* 0 data
        log.setRange(0, 30, size);
        while (!end) {
            int segmentSize = 0;
            TreeMap<Long, byte[]> map = new TreeMap<>();
            while (segmentSize < bufferSize) {
                Chunk c = Chunk.read(dataIn, false);
                if (c == null) {
                    end = true;
                    break;
                }
                int length = c.value.length;
                log.printProgress(length);
                segmentSize += length;
                for (long x : c.idList) {
                    map.put(x, c.value);
                }
            }
            if (map.size() == 0) {
                break;
            }
            segmentStart.add(outPos);
            for (Long x : map.keySet()) {
                outPos += writeVarLong(tempOut, x);
                outPos += writeVarLong(tempOut, 0);
                byte[] v = map.get(x);
                outPos += writeVarLong(tempOut, v.length);
                tempOut.write(v);
                outPos += v.length;
            }
            outPos += writeVarLong(tempOut, 0);
        }
        tempOut.close();
        long tempSize = new File(tempFileName).length();
        size = outPos;

        // merge blocks if needed
        int blockSize = 64;
        boolean merge = false;
        while (segmentStart.size() > blockSize) {
            merge = true;
            log.setRange(30, 50, tempSize);
            log.println();
            log.println("Merging " + segmentStart.size() + " segments " + blockSize + ":1");
            ArrayList<Long> segmentStart2 = new ArrayList<>();
            outPos = 0;
            DataOutputStream tempOut2 = new DataOutputStream(new BufferedOutputStream(
                    new FileOutputStream(tempFileName + ".b"), 1024 * 1024));
            while (segmentStart.size() > 0) {
                segmentStart2.add(outPos);
                int s = Math.min(segmentStart.size(), blockSize);
                List<Long> start = segmentStart.subList(0, s);
                TreeSet<ChunkStream> segmentIn = new TreeSet<>();
                long read = openSegments(start, segmentIn, tempFileName, false);
                log.printProgress(read);

                Iterator<Chunk> it = merge(segmentIn, log);
                while (it.hasNext()) {
                    Chunk c = it.next();
                    outPos += writeVarLong(tempOut2, c.idList.get(0));
                    outPos += writeVarLong(tempOut2, 0);
                    outPos += writeVarLong(tempOut2, c.value.length);
                    tempOut2.write(c.value);
                    outPos += c.value.length;
                }
                outPos += writeVarLong(tempOut2, 0);

                segmentStart = segmentStart.subList(s, segmentStart.size());
            }
            segmentStart = segmentStart2;
            tempOut2.close();
            tempSize = new File(tempFileName).length();
            new File(tempFileName).delete();
            tempFileName += ".b";
        }
        if (merge) {
            log.println();
            log.println("Combining " + segmentStart.size() + " segments");
        }

        TreeSet<ChunkStream> segmentIn = new TreeSet<>();
        DataOutputStream dataOut = new DataOutputStream(out);
        log.setRange(50, 100, size);

        long read = openSegments(segmentStart, segmentIn, tempFileName, false);
        log.printProgress(read);

        Iterator<Chunk> it = merge(segmentIn, log);
        while (it.hasNext()) {
            dataOut.write(it.next().value);
        }
        new File(tempFileName).delete();
        dataOut.flush();
    }

    /**
     * A stream of chunks.
     */
    static class ChunkStream implements Comparable<ChunkStream> {
        final int id;
        Chunk current;
        DataInputStream in;
        boolean readKey;

        ChunkStream(int id) {
            this.id = id;
        }

        /**
         * Read the next chunk.
         *
         * @return the number of bytes read
         */
        int readNext() {
            current = null;
            current = Chunk.read(in, readKey);
            if (current == null) {
                return 0;
            }
            return current.value.length;
        }

        @Override
        public int compareTo(ChunkStream o) {
            int comp = current.compareTo(o.current);
            if (comp != 0) {
                return comp;
            }
            return Integer.signum(id - o.id);
        }
    }

    /**
     * A chunk of data.
     */
    static class Chunk implements Comparable<Chunk> {
        ArrayList<Long> idList;
        final byte[] value;
        private final int[] sortKey;

        Chunk(ArrayList<Long> idList, int[] sortKey, byte[] value) {
            this.idList = idList;
            this.sortKey = sortKey;
            this.value = value;
        }

        /**
         * Read a chunk.
         *
         * @param in the input stream
         * @param readKey whether to read the sort key
         * @return the chunk, or null if 0 has been read
         */
        public static Chunk read(DataInputStream in, boolean readKey) {
            try {
                ArrayList<Long> idList = new ArrayList<>();
                while (true) {
                    long x = readVarLong(in);
                    if (x == 0) {
                        break;
                    }
                    idList.add(x);
                }
                if (idList.size() == 0) {
                    // eof
                    in.close();
                    return null;
                }
                int[] key = null;
                if (readKey) {
                    key = new int[4];
                    for (int i = 0; i < key.length; i++) {
                        key[i] = in.readInt();
                    }
                }
                int len = (int) readVarLong(in);
                byte[] value = new byte[len];
                in.readFully(value);
                return new Chunk(idList, key, value);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * Write a chunk.
         *
         * @param out the output stream
         * @param writeKey whether to write the sort key
         * @return the number of bytes written
         */
        int write(DataOutputStream out, boolean writeKey) throws IOException {
            int len = 0;
            for (long x : idList) {
                len += writeVarLong(out, x);
            }
            len += writeVarLong(out, 0);
            if (writeKey) {
                for (int i = 0; i < sortKey.length; i++) {
                    out.writeInt(sortKey[i]);
                    len += 4;
                }
            }
            len += writeVarLong(out, value.length);
            out.write(value);
            len += value.length;
            return len;
        }

        @Override
        public int compareTo(Chunk o) {
            if (sortKey == null) {
                // sort by id
                long a = idList.get(0);
                long b = o.idList.get(0);
                if (a < b) {
                    return -1;
                } else if (a > b) {
                    return 1;
                }
                return 0;
            }
            for (int i = 0; i < sortKey.length; i++) {
                if (sortKey[i] < o.sortKey[i]) {
                    return -1;
                } else if (sortKey[i] > o.sortKey[i]) {
                    return 1;
                }
            }
            if (value.length < o.value.length) {
                return -1;
            } else if (value.length > o.value.length) {
                return 1;
            }
            for (int i = 0; i < value.length; i++) {
                int a = value[i] & 255;
                int b = o.value[i] & 255;
                if (a < b) {
                    return -1;
                } else if (a > b) {
                    return 1;
                }
            }
            return 0;
        }
    }

    /**
     * A logger, including context.
     */
    static class Log {

        private long lastTime;
        private long current;
        private int pos;
        private int low;
        private int high;
        private long total;

        /**
         * Print an empty line.
         */
        void println() {
            System.out.println();
            pos = 0;
        }

        /**
         * Print a message.
         *
         * @param msg the message
         */
        void print(String msg) {
            System.out.print(msg);
        }

        /**
         * Print a message.
         *
         * @param msg the message
         */
        void println(String msg) {
            System.out.println(msg);
            pos = 0;
        }

        /**
         * Set the range.
         *
         * @param low the percent value if current = 0
         * @param high the percent value if current = total
         * @param total the maximum value
         */
        void setRange(int low, int high, long total) {
            this.low = low;
            this.high = high;
            this.current = 0;
            this.total = total;
        }

        /**
         * Print the progress.
         *
         * @param offset the offset since the last operation
         */
        void printProgress(long offset) {
            current += offset;
            long now = System.nanoTime();
            if (now - lastTime > TimeUnit.SECONDS.toNanos(3)) {
                String msg = (low + (high - low) * current / total) + "% ";
                if (pos > 80) {
                    System.out.println();
                    pos = 0;
                }
                System.out.print(msg);
                pos += msg.length();
                lastTime = now;
            }
        }

    }

    /**
     * Write a variable size long value.
     *
     * @param out the output stream
     * @param x the value
     * @return the number of bytes written
     */
    static int writeVarLong(OutputStream out, long x)
            throws IOException {
        int len = 0;
        while ((x & ~0x7f) != 0) {
            out.write((byte) (0x80 | (x & 0x7f)));
            x >>>= 7;
            len++;
        }
        out.write((byte) x);
        return ++len;
    }

    /**
     * Read a variable size long value.
     *
     * @param in the input stream
     * @return the value
     */
    static long readVarLong(InputStream in) throws IOException {
        long x = in.read();
        if (x < 0) {
            throw new EOFException();
        }
        x = (byte) x;
        if (x >= 0) {
            return x;
        }
        x &= 0x7f;
        for (int s = 7; s < 64; s += 7) {
            long b = in.read();
            if (b < 0) {
                throw new EOFException();
            }
            b = (byte) b;
            x |= (b & 0x7f) << s;
            if (b >= 0) {
                break;
            }
        }
        return x;
    }

    /**
     * An input stream that uses a shared file channel.
     */
    static class SharedInputStream extends InputStream {
        private final FileChannel channel;
        private final long endPosition;
        private long position;

        SharedInputStream(FileChannel channel, long position, long endPosition) {
            this.channel = channel;
            this.position = position;
            this.endPosition = endPosition;
        }

        @Override
        public int read() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (len == 0) {
                return 0;
            }
            len = (int) Math.min(len, endPosition - position);
            if (len <= 0) {
                return -1;
            }
            ByteBuffer buff = ByteBuffer.wrap(b, off, len);
            len = channel.read(buff, position);
            position += len;
            return len;
        }

    }

}

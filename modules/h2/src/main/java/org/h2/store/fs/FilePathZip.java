/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.h2.message.DbException;
import org.h2.util.IOUtils;
import org.h2.util.New;

/**
 * This is a read-only file system that allows
 * to access databases stored in a .zip or .jar file.
 */
public class FilePathZip extends FilePath {

    @Override
    public FilePathZip getPath(String path) {
        FilePathZip p = new FilePathZip();
        p.name = path;
        return p;
    }

    @Override
    public void createDirectory() {
        // ignore
    }

    @Override
    public boolean createFile() {
        throw DbException.getUnsupportedException("write");
    }

    @Override
    public void delete() {
        throw DbException.getUnsupportedException("write");
    }

    @Override
    public boolean exists() {
        try {
            String entryName = getEntryName();
            if (entryName.length() == 0) {
                return true;
            }
            try (ZipFile file = openZipFile()) {
                return file.getEntry(entryName) != null;
            }
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    public long lastModified() {
        return 0;
    }

    @Override
    public FilePath getParent() {
        int idx = name.lastIndexOf('/');
        return idx < 0 ? null : getPath(name.substring(0, idx));
    }

    @Override
    public boolean isAbsolute() {
        String fileName = translateFileName(name);
        return FilePath.get(fileName).isAbsolute();
    }

    @Override
    public FilePath unwrap() {
        return FilePath.get(name.substring(getScheme().length() + 1));
    }

    @Override
    public boolean isDirectory() {
        try {
            String entryName = getEntryName();
            if (entryName.length() == 0) {
                return true;
            }
            try (ZipFile file = openZipFile()) {
                Enumeration<? extends ZipEntry> en = file.entries();
                while (en.hasMoreElements()) {
                    ZipEntry entry = en.nextElement();
                    String n = entry.getName();
                    if (n.equals(entryName)) {
                        return entry.isDirectory();
                    } else  if (n.startsWith(entryName)) {
                        if (n.length() == entryName.length() + 1) {
                            if (n.equals(entryName + "/")) {
                                return true;
                            }
                        }
                    }
                }
            }
            return false;
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    public boolean canWrite() {
        return false;
    }

    @Override
    public boolean setReadOnly() {
        return true;
    }

    @Override
    public long size() {
        try {
            try (ZipFile file = openZipFile()) {
                ZipEntry entry = file.getEntry(getEntryName());
                return entry == null ? 0 : entry.getSize();
            }
        } catch (IOException e) {
            return 0;
        }
    }

    @Override
    public ArrayList<FilePath> newDirectoryStream() {
        String path = name;
        ArrayList<FilePath> list = New.arrayList();
        try {
            if (path.indexOf('!') < 0) {
                path += "!";
            }
            if (!path.endsWith("/")) {
                path += "/";
            }
            try (ZipFile file = openZipFile()) {
                String dirName = getEntryName();
                String prefix = path.substring(0, path.length() - dirName.length());
                Enumeration<? extends ZipEntry> en = file.entries();
                while (en.hasMoreElements()) {
                    ZipEntry entry = en.nextElement();
                    String name = entry.getName();
                    if (!name.startsWith(dirName)) {
                        continue;
                    }
                    if (name.length() <= dirName.length()) {
                        continue;
                    }
                    int idx = name.indexOf('/', dirName.length());
                    if (idx < 0 || idx >= name.length() - 1) {
                        list.add(getPath(prefix + name));
                    }
                }
            }
            return list;
        } catch (IOException e) {
            throw DbException.convertIOException(e, "listFiles " + path);
        }
    }

    @Override
    public InputStream newInputStream() throws IOException {
        return new FileChannelInputStream(open("r"), true);
    }

    @Override
    public FileChannel open(String mode) throws IOException {
        ZipFile file = openZipFile();
        ZipEntry entry = file.getEntry(getEntryName());
        if (entry == null) {
            file.close();
            throw new FileNotFoundException(name);
        }
        return new FileZip(file, entry);
    }

    @Override
    public OutputStream newOutputStream(boolean append) throws IOException {
        throw new IOException("write");
    }

    @Override
    public void moveTo(FilePath newName, boolean atomicReplace) {
        throw DbException.getUnsupportedException("write");
    }

    private static String translateFileName(String fileName) {
        if (fileName.startsWith("zip:")) {
            fileName = fileName.substring("zip:".length());
        }
        int idx = fileName.indexOf('!');
        if (idx >= 0) {
            fileName = fileName.substring(0, idx);
        }
        return FilePathDisk.expandUserHomeDirectory(fileName);
    }

    @Override
    public FilePath toRealPath() {
        return this;
    }

    private String getEntryName() {
        int idx = name.indexOf('!');
        String fileName;
        if (idx <= 0) {
            fileName = "";
        } else {
            fileName = name.substring(idx + 1);
        }
        fileName = fileName.replace('\\', '/');
        if (fileName.startsWith("/")) {
            fileName = fileName.substring(1);
        }
        return fileName;
    }

    private ZipFile openZipFile() throws IOException {
        String fileName = translateFileName(name);
        return new ZipFile(fileName);
    }

    @Override
    public FilePath createTempFile(String suffix, boolean deleteOnExit,
            boolean inTempDir) throws IOException {
        if (!inTempDir) {
            throw new IOException("File system is read-only");
        }
        return new FilePathDisk().getPath(name).createTempFile(suffix,
                deleteOnExit, true);
    }

    @Override
    public String getScheme() {
        return "zip";
    }

}

/**
 * The file is read from a stream. When reading from start to end, the same
 * input stream is re-used, however when reading from end to start, a new input
 * stream is opened for each request.
 */
class FileZip extends FileBase {

    private static final byte[] SKIP_BUFFER = new byte[1024];

    private final ZipFile file;
    private final ZipEntry entry;
    private long pos;
    private InputStream in;
    private long inPos;
    private final long length;
    private boolean skipUsingRead;

    FileZip(ZipFile file, ZipEntry entry) {
        this.file = file;
        this.entry = entry;
        length = entry.getSize();
    }

    @Override
    public long position() {
        return pos;
    }

    @Override
    public long size() {
        return length;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        seek();
        int len = in.read(dst.array(), dst.arrayOffset() + dst.position(),
                dst.remaining());
        if (len > 0) {
            dst.position(dst.position() + len);
            pos += len;
            inPos += len;
        }
        return len;
    }

    private void seek() throws IOException {
        if (inPos > pos) {
            if (in != null) {
                in.close();
            }
            in = null;
        }
        if (in == null) {
            in = file.getInputStream(entry);
            inPos = 0;
        }
        if (inPos < pos) {
            long skip = pos - inPos;
            if (!skipUsingRead) {
                try {
                    IOUtils.skipFully(in, skip);
                } catch (NullPointerException e) {
                    // workaround for Android
                    skipUsingRead = true;
                }
            }
            if (skipUsingRead) {
                while (skip > 0) {
                    int s = (int) Math.min(SKIP_BUFFER.length, skip);
                    s = in.read(SKIP_BUFFER, 0, s);
                    skip -= s;
                }
            }
            inPos = pos;
        }
    }

    @Override
    public FileChannel position(long newPos) {
        this.pos = newPos;
        return this;
    }

    @Override
    public FileChannel truncate(long newLength) throws IOException {
        throw new IOException("File is read-only");
    }

    @Override
    public void force(boolean metaData) throws IOException {
        // nothing to do
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        throw new IOException("File is read-only");
    }

    @Override
    public synchronized FileLock tryLock(long position, long size,
            boolean shared) throws IOException {
        if (shared) {
            return new FileLock(new FakeFileChannel(), position, size, shared) {

                @Override
                public boolean isValid() {
                    return true;
                }

                @Override
                public void release() throws IOException {
                    // ignore
                }};
        }
        return null;
    }

    @Override
    protected void implCloseChannel() throws IOException {
        if (in != null) {
            in.close();
            in = null;
        }
        file.close();
    }

}

package org.apache.ignite.internal.processors.igfs;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileStore;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.IgniteException;
import org.apache.ignite.igfs.IgfsDirectoryNotEmptyException;
import org.apache.ignite.igfs.IgfsException;
import org.apache.ignite.igfs.IgfsFile;
import org.apache.ignite.igfs.IgfsParentNotDirectoryException;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.igfs.IgfsPathAlreadyExistsException;
import org.apache.ignite.igfs.secondary.IgfsSecondaryFileSystem;
import org.apache.ignite.igfs.secondary.IgfsSecondaryFileSystemPositionedReadable;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.igfs.IgfsEx.PROP_GROUP_NAME;
import static org.apache.ignite.internal.processors.igfs.IgfsEx.PROP_PERMISSION;
import static org.apache.ignite.internal.processors.igfs.IgfsEx.PROP_USER_NAME;

/**
 * Implementation based on {@link java.io.File}.
 */
public class FileIgfsSecondaryFileSystemImpl implements IgfsSecondaryFileSystem {
    /** The root of the file system. */
    private final Path rootNioPath;

    /** The String canonical path of */
    private final String rootCanonicalPath;

    /**
     * Creates new file system.
     *
     * @param rootDirPath The full absolute path to the root of the file system.
     */
    public FileIgfsSecondaryFileSystemImpl(String rootDirPath) throws IOException {
        A.notNull(rootDirPath, "rootDirPath");
        //assert rootDirPath != null;

        rootNioPath = Paths.get(rootDirPath).normalize();

        //rootDir.mkdirs();

        Files.createDirectories(rootNioPath);

//        assert rootDir.exists();
//        assert rootDir.isDirectory();

        if (!Files.exists(rootNioPath) || !Files.isDirectory(rootNioPath))
            throw new IOException("Missing or bad root directory. [path=" + rootNioPath + ']');
//
//        this.rootDir = rootDir;

        this.rootCanonicalPath = rootNioPath.toString(); //rootDir.getCanonicalPath();
    }

//    /**
//     *
//     * @param path
//     * @return
//     */
//    @Deprecated
//    private File asFile(IgfsPath path) {
//        return new File(rootDir, path.toString());
//    }

    /**
     *
     * @param path
     * @return
     */
    private Path asNioPath(IgfsPath path) {
        //return Paths.get(rootDir.getAbsolutePath(), path.toString());
        //String p = path.toString();

        String sep = rootNioPath.getFileSystem().getSeparator();

        String x;

        if (rootCanonicalPath.endsWith(sep) || path.toString().startsWith(sep)) {
            x = rootCanonicalPath + path;
        } else {
            x = rootCanonicalPath + sep + path;
        }

        return Paths.get(x);
    }

    /**
     *
     * @param p The path.
     * @return
     */
    private IgfsFile asIgfsFile(Path p) {
        //return new FileIgfsFile(f, rootCanonicalPath);
        int rootLen = rootCanonicalPath.length();

        final String igfsPath;

        if (rootLen <= 1)
            igfsPath = p.toString();
        else
            igfsPath = p.toString().substring(rootLen);

        return new FileIgfsFile(p.toString(), igfsPath);
    }

    /** {@inheritDoc} */
    @Override public OutputStream append(IgfsPath path, int bufSize, boolean create, @Nullable Map<String, String>
        props) throws IgniteException {
        //File f = asFile(path);
        Path p = asNioPath(path);

//        if (!Files.exists(p) && !create)
//            throw new IgfsPathNotFoundException("File not found. [file=" + path + ']');

        try {

            OutputStream os;
            if (create)
                os = Files.newOutputStream(p, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            else
                os = Files.newOutputStream(p, StandardOpenOption.APPEND);

            if (bufSize > 0)
                os = new BufferedOutputStream(os, bufSize);

            setProperties(p, props);

            return os;
        }
        catch (IOException ioe) {
            throw new IgniteException(ioe);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean exists(IgfsPath path) {
        Path p = asNioPath(path);

        return Files.exists(p);
    }

    /** {@inheritDoc} */
    @Override public IgfsFile update(IgfsPath path, Map<String, String> props) throws IgniteException {
        Path p = asNioPath(path);

        if (!Files.exists(p))
            return null;

        try {
            setProperties(p, props);
        }
        catch (IOException ioe) {
            throw new IgniteException(ioe);
        }

        return info(path);
    }

    /** {@inheritDoc} */
    @Override public void rename(IgfsPath src, IgfsPath dest) throws IgniteException {
        Path srcPath = asNioPath(src);
        Path dstPath = asNioPath(dest);

        if (Files.exists(dstPath)) {
            if (Files.isDirectory(dstPath))
                // This operation assumes contract that is different from
                // java.nio.file.Files.move() contract:
                // If the destination exists and it is a directory,
                // the source is to be placed *into* it. So, create a new unexisting destination:
                dstPath = dstPath.resolve(src.name());
            else
                throw new IgfsPathAlreadyExistsException("[path=" + dest + ']');
        }

        try {
            Path rslt = Files.move(srcPath, dstPath, StandardCopyOption.ATOMIC_MOVE);

            assert dstPath.equals(rslt);
        }
        catch (IOException ioe) {
            throw new IgniteException(ioe);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean delete(IgfsPath path, boolean recursive) throws IgniteException {
//        File victim = asFile(path);
//
//        if (recursive && victim.isDirectory())
//            return U.delete(victim);
//        else
//            return victim.delete();

        final Path victim = asNioPath(path);

        try {
            if (recursive) {
                final boolean[] rslt = new boolean[] { false };

                Files.walkFileTree(victim,
                    new FileVisitor<Path>() {
                        @Override public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws
                            IOException {
                            boolean deleted = Files.deleteIfExists(dir);

                            if (victim.equals(dir))
                                rslt[0] = deleted;

                            return FileVisitResult.CONTINUE;
                        }

                        @Override public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws
                            IOException {
                            Files.delete(file);

                            return FileVisitResult.CONTINUE;
                        }

                        @Override public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {

                            return FileVisitResult.TERMINATE;
                        }

                        @Override public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
                            throws IOException {
                            return FileVisitResult.CONTINUE;
                        }
                    });
                return rslt[0];
            }
            else {
                try {
                    return Files.deleteIfExists(victim);
                }
                catch (DirectoryNotEmptyException dnee) {
                    throw new IgfsDirectoryNotEmptyException(dnee);
                }
            }
        }
        catch (IOException ioe) {
            throw new IgfsException(ioe);
        }
    }

    /** {@inheritDoc} */
    @Override public void mkdirs(IgfsPath path) throws IgniteException {
        mkdirs(path, null);
    }

    /** {@inheritDoc} */
    @Override public void mkdirs(IgfsPath path, @Nullable Map<String, String> props) throws IgniteException {
        try {
            Path target = asNioPath(path);

            if (Files.exists(target)) {
                if (!Files.isDirectory(target))
                    throw new IgfsParentNotDirectoryException(path.toString());
            }
            else {
                IOException e = null;

                try {
                    Files.createDirectories(target);
                }
                catch (IOException ioe) {
                    e = ioe;
                }

                if (!Files.exists(target)) {
                    Path x = target;

                    while (true) {
                        x = x.getParent();

                        if (x == null)
                            break;

                        if (!Files.isDirectory(x))
                            throw new IgfsParentNotDirectoryException("[path=" + x.toString() + ']');
                    }

                    if (e == null)
                        throw new IgfsException("Failed to create directory [dir=" + target + ']');
                    else
                        throw new IgfsException(e);
                }
            }

            // TODO: all the implicitly created directories should also have the specified permissions?
            setProperties(target, props);
        }
        catch (IOException ioe) {
            throw new IgniteException(ioe);
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<IgfsPath> listPaths(IgfsPath path) throws IgniteException {
        Path p = asNioPath(path);

        try {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(p)) {

                if (stream == null)
                    return Collections.emptyList();

                List<IgfsPath> list = new ArrayList<>();

                Iterator<Path> it = stream.iterator();

                while (it.hasNext())
                    list.add(asIgfsFile(it.next()).path());

                return list;
            }
        }
        catch (IOException ioe) {
            throw new IgfsException(ioe);
        }
//        //Path p;
//        //p.iterator()
//        Files.
//
//        File file = asFile(path);
//
//        File[] ff = file.listFiles();
//
//        if (ff == null)
//            return Collections.emptyList();
//
//        List<IgfsPath> list = new ArrayList<>(ff.length);
//
//        for (File f: ff)
//            list.add(asIgfsFile(f).path());
//
//        return list;
    }

    /** {@inheritDoc} */
    @Override public Collection<IgfsFile> listFiles(IgfsPath path) throws IgniteException {
        Path p = asNioPath(path);

        try {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(p)) {

                if (stream == null)
                    return Collections.emptyList();

                List<IgfsFile> list = new ArrayList<>();

                Iterator<Path> it = stream.iterator();

                while (it.hasNext())
                    list.add(asIgfsFile(it.next()));

                return list;
            }
        }
        catch (IOException ioe) {
            throw new IgfsException(ioe);
        }

//        // -------------------------------
//        File file = asFile(path);
//
//        File[] ff = file.listFiles();
//
//        if (ff == null)
//            return Collections.emptyList();
//
//        List<IgfsFile> list = new ArrayList<>(ff.length);
//
//        for (File f: ff)
//            list.add(asIgfsFile(f));
//
//        return list;
    }

    /**
     * Note: 'bufSize' is not used by this implementation.
     */
    @Override public IgfsSecondaryFileSystemPositionedReadable open(IgfsPath path, int bufSize) throws IgniteException {
        Path p = asNioPath(path);

        try {
            RandomAccessFile raf = new RandomAccessFile(p.toString(), "r");

            return new RandomAccessFileIgfsSecondaryFileSystemPositionedReadable(raf);
        }
        catch (IOException ioe) {
            throw new IgniteException(ioe);
        }
    }

    /**
     * PositionedReadable implementation based on {@link RandomAccessFile}.
     * Life cycle note: the underlying RandomAccessFile will be closed
     * upon this PositionedReadable closing.
     */
    static class RandomAccessFileIgfsSecondaryFileSystemPositionedReadable
            implements IgfsSecondaryFileSystemPositionedReadable, Closeable {
        /** The random access file we delegate read requests to. */
        private final RandomAccessFile raf;

        /**
         * Creates new readable.
         */
        RandomAccessFileIgfsSecondaryFileSystemPositionedReadable(RandomAccessFile raf) {
            assert raf != null;

            this.raf = raf;
        }

        /** {@inheritDoc} */
        @Override public int read(long pos, byte[] buf, int off, int len) throws IOException {
            raf.seek(pos);

            return raf.read(buf, off, len);
        }

        /** {@inheritDoc} */
        @Override public void close() throws IOException {
            raf.close();
        }
    }

    /** {@inheritDoc} */
    @Override public OutputStream create(IgfsPath path, boolean overwrite) throws IgniteException {
        return create(path, 0, overwrite, 0, 0, null);
    }

    /** {@inheritDoc} */
    @Override public OutputStream create(IgfsPath path, int bufSize, boolean overwrite, int replication, long
        blockSize, @Nullable Map<String, String> props) throws IgniteException {
        //File f = asFile(path);
        Path p = asNioPath(path);

//        if (f.exists()
//            && !overwrite)
//            throw new IgfsPathAlreadyExistsException("File already exists. [path=" + path + ']');

        Path parentFile = p.getParent(); //f.getParentFile();

        try {
            if (!Files.exists(parentFile))
                Files.createDirectories(parentFile);

            OutputStream os;

            if (overwrite)
                os = Files.newOutputStream(p, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            else
                os = Files.newOutputStream(p, StandardOpenOption.CREATE_NEW);

            if (bufSize > 0)
                os = new BufferedOutputStream(os, bufSize);

            setProperties(p, props);

            return os;
        }
        catch (IOException ioe) {
            throw new IgfsException(ioe);
        }
    }

    /**
     * Sets properties to the specified file.
     *
     * @param nioPath The file to set properties for.
     * @param props The properties to set. Null properties mean to set nothing.
     * @throws IOException On I/O error.
     */
    static void setProperties(Path nioPath, @Nullable Map<String, String> props) throws IOException {
        if (props == null)
            return;

        String decimalStr = props.get(PROP_PERMISSION);

        if (decimalStr == null)
            return;

        int bitPerm = decimalsToBitPermissions(decimalStr);

        Set<PosixFilePermission> permSet = toPermSet(bitPerm);

        Files.setPosixFilePermissions(nioPath, permSet);
    }

    /**
     * Gets properties of the given file. Map return an empty collection, but never returns null.
     *
     * @param p The File to process.
     * @return The map of the properties.
     * @throws IOException On I/O error.
     */
    static Map<String, String> getProperties(Path p) throws IOException {
        Map<String,String> map = new HashMap<>(3);

        String perm = readPermissions(p);

        if (perm != null)
            map.put(PROP_PERMISSION, perm);

        PosixFileAttributes pa = Files.readAttributes(p, PosixFileAttributes.class);

        if (pa != null) {
            if (pa.owner() != null)
                map.put(PROP_USER_NAME, pa.owner().getName());

            if (pa.group() != null)
                map.put(PROP_GROUP_NAME, pa.group().getName());
        }

        return Collections.unmodifiableMap(map);
    }

    /**
     * Reads permission from a file.
     *
     * @param p The path to read permissions for.
     * @return  The permission code in 4 decimal digit form, e.g. "0775".
     */
    static String readPermissions(Path p) throws IOException {
        Set<PosixFilePermission> set = Files.getPosixFilePermissions(p);

        int bitPerm = toBitPerm(set);

        return bitPermissionsToDecimals(bitPerm);
    }

    /**
     *
     * @param set
     * @return
     */
    static int toBitPerm(Set<PosixFilePermission> set) {
        int bitPerm = 0;

        for (PosixFilePermission posix: PosixFilePermission.values()) {
            if (set.contains(posix))
                bitPerm |= (1 << (PosixFilePermission.values().length - posix.ordinal() - 1));
        }

        return bitPerm;
    }

    /**
     *
     * @param bitPerm
     * @return
     */
    static String bitPermissionsToDecimals(int bitPerm) {
        final int digits = 4; // process 12 bits == 4 * bit-triplets.

        final char[] rslt = new char[digits];

        for (int i=0; i<digits; i++) {
            int threeBits = (bitPerm >>> (i * 3)) & 7;

            rslt[digits - 1 - i] = Integer.toString(threeBits).charAt(0);
        }

        return new String(rslt);
    }

    static int decimalsToBitPermissions(String decimals) {
        assert decimals.length() >= 3;

        int i = decimals.length() - 1;

        int rslt = 0;

        while (true) {
            char c = decimals.charAt(i);

            int k = Integer.parseInt("" + c);

            rslt |= (k << (3 * (decimals.length() - 1 - i)));

            i--;

            if (i == 0 || i <= decimals.length() - 4)
                break;
        }

        return rslt;
    }
//
//    public static void main(String[] args) {
//        //toPermSet();
//    }

//    /**
//     * Sets permission to the file.
//     *
//     * @param p The file to set permissions for.
//     * @param perm The permission code in 4 decimal digit form, e.g. "0775".
//     */
//    static void writePermissions(Path p, String perm) {
////        String ownerModeDigit = perm.substring(perm.length() - 3, perm.length() - 2);
////
////        int ownerRwx = Integer.parseInt(ownerModeDigit);
////
////        assert ownerRwx <= 7;
////
////        f.setReadable((ownerRwx & 4) != 0, false/*ownerOnly*/);
////        f.setWritable((ownerRwx & 2) != 0, false/*ownerOnly*/);
////        f.setExecutable((ownerRwx & 1) != 0, false/*ownerOnly*/);
//
//        int bitPerm = decimalsToBitPermissions(perm);
//
//        Set<PosixFilePermission> set = toPermSet(bitPerm);
//
//        try {
//            Files.setPosixFilePermissions(p, set);
//        }
//        catch (IOException ioe) {
//            throw new IgfsException(ioe);
//        }
//    }

    /**
     *
     * @param bitPerm
     * @return
     */
    static Set<PosixFilePermission> toPermSet(int bitPerm) {
        Set<PosixFilePermission> set = new HashSet<>();

        for (int i=0; i<9; i++) {
            if ((bitPerm & (1 << i)) != 0)
                set.add(PosixFilePermission.values()[8 - i]);
        }

        return set;
    }

    /** {@inheritDoc} */
    @Override public IgfsFile info(IgfsPath path) throws IgniteException {
        Path p = asNioPath(path);

        if (!Files.exists(p))
            return null;

        return new FileIgfsFile(p.toString(), path.toString());
    }

    /** {@inheritDoc} */
    @Override public long usedSpaceSize() throws IgniteException {
        try {
            Iterator<FileStore> it
                = rootNioPath.getFileSystem().getFileStores().iterator();

            FileStore store;

            long total = 0;
            long used = 0;

            while (it.hasNext()) {
                store = it.next();

                total += store.getTotalSpace();

                used += store.getUnallocatedSpace();
            }

            return total - used;
        }
        catch (IOException ioe) {
            throw new IgfsException(ioe);
        }
//
//        File rootDirFile = new File(rootCanonicalPath);
//
//        // TODO: This is not guaranteed to be accurate.
//        // TODO: May be it is better to calculate the size summing up
//        // TODO:   sizes of all the files?
//        return rootDirFile.getTotalSpace() - rootDirFile.getUsableSpace();
    }

    /** {@inheritDoc} */
    @Override public Map<String, String> properties() {
        // Properties of the file system are empty.
        return Collections.emptyMap();
    }

    /** {@inheritDoc} */
    @Override public void close() throws IgniteException {
        // Noop
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        // By contract this #toString() implementation returns
        // the canonical path of this file system.
        return rootCanonicalPath;
    }
}
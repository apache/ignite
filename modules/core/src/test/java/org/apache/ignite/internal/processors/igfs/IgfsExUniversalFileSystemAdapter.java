package org.apache.ignite.internal.processors.igfs;

import org.apache.ignite.igfs.*;

import java.io.*;
import java.util.*;

/**
 * Universal adapter over {@link IgfsEx} filesystem.
 */
public class IgfsExUniversalFileSystemAdapter implements UniversalFileSystemAdapter {

    /** The wrapped igfs. */
    private final IgfsEx igfsEx;

    /**
     * Constructor.
     * @param igfsEx the igfs to be wrapped.
     */
    public IgfsExUniversalFileSystemAdapter(IgfsEx igfsEx) {
        this.igfsEx = igfsEx;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return igfsEx.name();
    }

    /** {@inheritDoc} */
    @Override public boolean exists(String path) {
        return igfsEx.exists(new IgfsPath(path));
    }

    /** {@inheritDoc} */
    @Override public void mkdirs(String path) throws IOException {
        igfsEx.mkdirs(new IgfsPath(path));
    }

    /** {@inheritDoc} */
    @Override public void format() throws IOException {
        igfsEx.format();
    }

    /** {@inheritDoc} */
    @Override public Map<String, String> properties(String path) {
        return igfsEx.info(new IgfsPath(path)).properties();
    }

    /** {@inheritDoc} */
    @Override public boolean delete(String path, boolean recursive) throws IOException {
        IgfsPath igfsPath = new IgfsPath(path);

        boolean del = igfsEx.delete(igfsPath, recursive);

        return del;
    }

    /** {@inheritDoc} */
    @Override public InputStream openInputStream(String path) throws IOException {
        IgfsPath igfsPath = new IgfsPath(path);

        IgfsInputStreamAdapter adapter = igfsEx.open(igfsPath);

        return adapter;
    }

    /** {@inheritDoc} */
    @Override public OutputStream openOutputStream(String path, boolean append) throws IOException {
        IgfsPath igfsPath = new IgfsPath(path);

        final IgfsOutputStream igfsOutputStream;
        if (append)
            igfsOutputStream = igfsEx.append(igfsPath, true/*create*/);
         else
            igfsOutputStream = igfsEx.create(igfsPath, true/*overwrite*/);

        return igfsOutputStream;
    }

    /** {@inheritDoc} */
    @Override public <T> T getAdapter(Class<T> clazz) {
        if (clazz == IgfsEx.class)
            return (T)igfsEx;

        return null;
    }
}

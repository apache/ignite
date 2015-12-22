package org.apache.ignite.internal.processors.hadoop.fs;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.igfs.HadoopFileSystemFactory;
import org.apache.ignite.internal.processors.hadoop.HadoopUtils;
import org.apache.ignite.internal.processors.igfs.IgfsPaths;
import org.apache.ignite.internal.processors.igfs.IgfsUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lifecycle.LifecycleAware;

import static org.apache.ignite.internal.util.lang.GridFunc.nullifyEmpty;

/**
 * The class is to be instantiated as a Spring beans, so it must have public zero-arg constructor.
 * The class is serializable as it will be transferred over the network as a part of {@link IgfsPaths} object.
 */
public class DefaultHadoopFileSystemFactory implements HadoopFileSystemFactory<FileSystem>, Externalizable, LifecycleAware {
    /** Configuration of the secondary filesystem, never null. */
    protected final Configuration cfg = HadoopUtils.safeCreateConfiguration();

    /** */
    private URI uri;

    /** Lazy per-user cache for the file systems. It is cleared and nulled in #close() method. */
    private final HadoopLazyConcurrentMap<String, FileSystem> fileSysLazyMap = new HadoopLazyConcurrentMap<>(
        new HadoopLazyConcurrentMap.ValueFactory<String, FileSystem>() {
            @Override public FileSystem createValue(String key) {
                try {
                    assert !F.isEmpty(key);

                    return createFileSystem(key);
                }
                catch (IOException ioe) {
                    throw new IgniteException(ioe);
                }
            }
        }
    );

    public DefaultHadoopFileSystemFactory() {
        //
    }

    @Override public FileSystem get(String userName) throws IOException {
        return fileSysLazyMap.getOrCreate(userName);
    }

    public void setUri(URI uri) {
        this.uri = uri;
    }

    /**
     * Convenience mathod, analog of {@link #setUri(URI)} with String type argument.
     * @param uriStr
     */
    public void setUri(String uriStr) {
        try {
            setUri(new URI(uriStr));
        }
        catch (URISyntaxException use) {
            throw new IgniteException(use);
        }
    }

    @Override public URI uri() {
        return uri;
    }

    /**
     * Configuration(s) setter, to be invoked from Spring config.
     * @param cfgPaths
     */
    public void setCfgPaths(Collection<String> cfgPaths) {
        cfgPaths = nullifyEmpty(cfgPaths);

        if (cfgPaths == null)
            return;

        for (String confPath: cfgPaths) {
            confPath = nullifyEmpty(confPath);

            if (confPath != null) {
                URL url = U.resolveIgniteUrl(confPath);

                if (url == null) {
                    // If secConfPath is given, it should be resolvable:
                    throw new IllegalArgumentException("Failed to resolve secondary file system configuration path " +
                        "(ensure that it exists locally and you have read access to it): " + confPath);
                }

                cfg.addResource(url);
            }
        }
    }


    protected void init() throws IOException {
        String secUri = nullifyEmpty(uri == null ? null : uri.toString());

        A.ensure(cfg != null, "config");

        // if secondary fs URI is not given explicitly, try to get it from the configuration:
        if (secUri == null)
            uri = FileSystem.getDefaultUri(cfg);
        else {
            try {
                uri = new URI(secUri);
            }
            catch (URISyntaxException use) {
                throw new IOException("Failed to resolve secondary file system URI: " + secUri);
            }
        }

        // Disable caching:
        String prop = HadoopFileSystemsUtils.disableFsCachePropertyName(uri.getScheme());

        cfg.setBoolean(prop, true);
    }

    /**
     * @return {@link org.apache.hadoop.fs.FileSystem}  instance for this secondary Fs.
     * @throws IOException
     */
    protected FileSystem createFileSystem(String userName) throws IOException {
        userName = IgfsUtils.fixUserName(nullifyEmpty(userName));

        final FileSystem fileSys;

        try {
            fileSys = FileSystem.get(uri, cfg, userName);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IOException("Failed to create file system due to interrupt.", e);
        }

        return fileSys;
    }

    @Override public void writeExternal(ObjectOutput out) throws IOException {
        cfg.write(out);

        U.writeString(out, uri.toString());
    }

    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        cfg.clear();

        cfg.readFields(in);

        String uriStr = U.readString(in);

        try {
            uri = new URI(uriStr);
        }
        catch (URISyntaxException use) {
            throw new IOException(use);
        }
    }

    @Override public void start() throws IgniteException {
        try {
            init();
        }
        catch (IOException ice) {
            throw new IgniteException(ice);
        }
    }

    @Override public void stop() throws IgniteException {
        try {
            fileSysLazyMap.close();
        }
        catch (IgniteCheckedException ice) {
            throw new IgniteException(ice);
        }
    }
}

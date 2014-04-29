/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.ggfs;

import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Description of path modes.
 */
public class GridGgfsPaths implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Secondary file system URI. */
    private String secondaryUri;

    /** Secondary file system configuration. */
    private String secondaryCfgPath;

    /** Default GGFS mode. */
    private GridGgfsMode dfltMode;

    /** Path modes. */
    private List<T2<GridGgfsPath, GridGgfsMode>> pathModes;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridGgfsPaths() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param secondaryUri Secondary file system URI.
     * @param secondaryCfgPath Secondary file system configuration path.
     * @param dfltMode Default GGFS mode.
     * @param pathModes Path modes.
     */
    public GridGgfsPaths(@Nullable String secondaryUri, @Nullable String secondaryCfgPath,
        GridGgfsMode dfltMode, @Nullable List<T2<GridGgfsPath, GridGgfsMode>> pathModes) {
        this.secondaryUri = secondaryUri;
        this.secondaryCfgPath = secondaryCfgPath;
        this.dfltMode = dfltMode;
        this.pathModes = pathModes;
    }

    /**
     * @return Secondary file system URI.
     */
    public String secondaryUri() {
        return secondaryUri;
    }

    /**
     * @return Secondary file system configuration.
     */
    public String secondaryConfigurationPath() {
        return secondaryCfgPath;
    }

    /**
     * @return Default GGFS mode.
     */
    public GridGgfsMode defaultMode() {
        return dfltMode;
    }

    /**
     * @return Path modes.
     */
    public List<T2<GridGgfsPath, GridGgfsMode>> pathModes() {
        return pathModes;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeString(out, secondaryUri);
        U.writeString(out, secondaryCfgPath);
        U.writeEnum0(out, dfltMode);

        if (pathModes != null) {
            out.writeBoolean(true);
            out.writeInt(pathModes.size());

            for (T2<GridGgfsPath, GridGgfsMode> pathMode : pathModes) {
                pathMode.getKey().writeExternal(out);
                U.writeEnum0(out, pathMode.getValue());
            }
        }
        else
            out.writeBoolean(false);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        secondaryUri = U.readString(in);
        secondaryCfgPath = U.readString(in);
        dfltMode = GridGgfsMode.fromOrdinal(U.readEnumOrdinal0(in));

        if (in.readBoolean()) {
            int size = in.readInt();

            pathModes = new ArrayList<>(size);

            for (int i = 0; i < size; i++) {
                GridGgfsPath path = new GridGgfsPath();
                path.readExternal(in);

                T2<GridGgfsPath, GridGgfsMode> entry = new T2<>(path, GridGgfsMode.fromOrdinal(U.readEnumOrdinal0(in)));

                pathModes.add(entry);
            }
        }
    }
}

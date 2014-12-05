/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.streamer;

import org.apache.ignite.lang.*;
import org.gridgain.grid.streamer.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 *
 */
public class GridStreamerAttributes implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private String name;

    /** Stages. */
    private Collection<IgniteBiTuple<String, String>> stages;

    /** At least once flag. */
    private boolean atLeastOnce;

    /** Max failover attempts. */
    private int maxFailoverAttempts;

    /** Max concurrent sessions. */
    private int maxConcurrentSes;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridStreamerAttributes() {
        // No-op.
    }

    /**
     * @param cfg Streamer configuration.
     */
    public GridStreamerAttributes(StreamerConfiguration cfg) {
        atLeastOnce = cfg.isAtLeastOnce();
        maxConcurrentSes = cfg.getMaximumConcurrentSessions();
        maxFailoverAttempts = cfg.getMaximumFailoverAttempts();
        name = cfg.getName();

        stages = new LinkedList<>();

        if (!F.isEmpty(cfg.getStages())) {
            for (GridStreamerStage stage : cfg.getStages())
                stages.add(F.t(stage.name(), stage.getClass().getName()));
        }
    }

    /**
     * @return Name.
     */
    @Nullable public String name() {
        return name;
    }

    /**
     * @return Streamer stages.
     */
    public Collection<IgniteBiTuple<String, String>> stages() {
        return stages;
    }

    /**
     * @return At least once flag.
     */
    public boolean atLeastOnce() {
        return atLeastOnce;
    }

    /**
     * @return Max failover attempts.
     */
    public int maxFailoverAttempts() {
        return maxFailoverAttempts;
    }

    /**
     * @return Max concurrent sessions.
     */
    public int maxConcurrentSessions() {
        return maxConcurrentSes;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeBoolean(atLeastOnce);
        out.writeInt(maxConcurrentSes);
        out.writeInt(maxFailoverAttempts);
        U.writeString(out, name);
        U.writeCollection(out, stages);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        atLeastOnce = in.readBoolean();
        maxConcurrentSes = in.readInt();
        maxFailoverAttempts = in.readInt();
        name = U.readString(in);
        stages = U.readCollection(in);
    }
}

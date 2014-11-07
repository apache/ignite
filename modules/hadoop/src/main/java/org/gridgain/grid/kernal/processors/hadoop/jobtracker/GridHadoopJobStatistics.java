/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.jobtracker;

import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.concurrent.atomic.*;

/**
 * Job runtime statistics.
 */
public class GridHadoopJobStatistics implements Externalizable {
    /** */
    private static final long serialVersionUID = 2100820892667831789L;

    /** */
    private static final int IDX_SUBMIT_START = 0;

    /** */
    private static final int IDX_SUBMIT_END = 1;

    /** */
    private static final int IDX_SETUP_START = 2;

    /** */
    private static final int IDX_SETUP_END = 3;

    /** */
    private static final int IDX_CLEANUP_START = 4;

    /** */
    private static final int IDX_CLEANUP_END = 5;

    /** */
    private static final int IDX_RESERVED_OFF = 10;

    /** */
    private AtomicLongArray arr;

    /** */
    private int mapTasks;

    /** */
    private int rdcTasks;

    /**
     * For {@link Externalizable}.
     */
    public GridHadoopJobStatistics() {
        // No-op.
    }

    /**
     * @param s1 The first source.
     * @param s2 The second source.
     */
    public GridHadoopJobStatistics(GridHadoopJobStatistics s1, @Nullable GridHadoopJobStatistics s2) {
        assert s1 != null;

        if (s2 != null) {
            assert s1.mapTasks == s2.mapTasks : s1.mapTasks + " " + s2.mapTasks;
            assert s1.rdcTasks == s2.rdcTasks;
            assert s1.arr.length() == s2.arr.length();
        }

        mapTasks = s1.mapTasks;
        rdcTasks = s1.rdcTasks;

        arr = new AtomicLongArray(s1.arr.length());

        for (int i = 0 ; i < arr.length(); i++) {
            long ts;

            if ((ts = s1.arr.get(i)) != 0)
                cas(i, ts);

            if (s2 != null && (ts = s2.arr.get(i)) != 0)
                cas(i, ts);
        }
    }

    /**
     * @param mapTasks Maps tasks number.
     * @param rdcTasks Reduce tasks number.
     */
    public GridHadoopJobStatistics(int mapTasks, int rdcTasks) {
        assert mapTasks > 0;
        assert rdcTasks >= 0;

        this.mapTasks = mapTasks;
        this.rdcTasks = rdcTasks;

        arr = new AtomicLongArray(IDX_RESERVED_OFF + mapTasks * 3 + rdcTasks * 4);
    }

    /**
     * @param time Timestamp.
     */
    public void onSubmitStart(long time) {
        cas(IDX_SUBMIT_START, time);
    }

    /**
     * Submit task.
     */
    public void onSubmitEnd() {
         cas(IDX_SUBMIT_END, U.currentTimeMillis());
    }

    /**
     * @param info Task info.
     * @param start Start or end of the task.
     */
    private void onTask(GridHadoopTaskInfo info, boolean start) {
        assert info.attempt() == 0;

        int idx;

        switch (info.type()) {
            case SETUP:
                idx = start ? IDX_SETUP_START : IDX_SETUP_END;

                break;

            case COMMIT:
            case ABORT:
                idx = start ? IDX_CLEANUP_START : IDX_CLEANUP_END;

                break;

            case MAP:
                idx = IDX_RESERVED_OFF + info.taskNumber();

                if (!start)
                    idx += 2 * mapTasks;

                break;

            case COMBINE:
                idx = IDX_RESERVED_OFF + mapTasks + info.taskNumber();

                break;

            case REDUCE:
                idx = IDX_RESERVED_OFF + 3 * mapTasks + info.taskNumber();

                if (!start)
                    idx += rdcTasks;

                break;

            default:
                throw new IllegalStateException("Task type: " + info.type());
        }

        cas(idx, U.currentTimeMillis());
    }

    /**
     * @param rdc Reducer.
     */
    public void onShuffleStart(int rdc) {
        cas(IDX_RESERVED_OFF + 3 * mapTasks + 2 * rdcTasks + rdc, U.currentTimeMillis());
    }

    /**
     * @param rdc Reducer.
     */
    public void onShuffleEnd(int rdc) {
        int idx = IDX_RESERVED_OFF + 3 * mapTasks + 3 * rdcTasks + rdc;

        // Don't cas because we don't know which message will be the last.
        arr.set(idx, U.currentTimeMillis());
    }

    /**
     * @param info Task info.
     */
    public void onTaskStart(GridHadoopTaskInfo info) {
        onTask(info, true);
    }

    /**
     * @param info Task info.
     */
    public void onTaskEnd(GridHadoopTaskInfo info) {
        onTask(info, false);
    }

    /**
     * @param idx Index.
     * @param time Timestamp.
     */
    private void cas(int idx, long time) {
        boolean res = arr.compareAndSet(idx, 0, time);

        while (!res) {
            long old = arr.get(idx);

            if (old < time)
                res = arr.compareAndSet(idx, old, time);
            else
                return;
        }
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(mapTasks);
        out.writeInt(rdcTasks);

        out.writeObject(arr);
    }

    /**
     * {@inheritDoc}
     */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        mapTasks = in.readInt();
        rdcTasks = in.readInt();

        arr = (AtomicLongArray)in.readObject();
    }

    /**
     * @param log Log.
     */
    public void print(GridLogger log) {
        long off = arr.get(IDX_SUBMIT_START);

        log.info(f("Submit: ", IDX_SUBMIT_START, IDX_SUBMIT_END, off));
        log.info(f("Setup: ", IDX_SETUP_START, IDX_SETUP_END, off));

        StringBuilder b = new StringBuilder();

        for (int i = 0; i < mapTasks; i++) {
            int start = IDX_RESERVED_OFF + i;
            int end = start + 2 * mapTasks;

            b.append("m");
            b.append(i);
            b.append(f("(", start, end, off));
            b.append(") ");
        }

        log.info("Map: " + b);

        b = new StringBuilder();

        for (int i = 0; i < mapTasks; i++) {
            int start = IDX_RESERVED_OFF  + mapTasks + i;
            int end = start + mapTasks;

            b.append("c");
            b.append(i);
            b.append(f("(", start, end, off));
            b.append(") ");
        }

        log.info("Combine: " + b);

        b = new StringBuilder();

        for (int i = 0; i < rdcTasks; i++) {
            int start = IDX_RESERVED_OFF  + 3 * mapTasks + 2 * rdcTasks + i;
            int end = start + rdcTasks;

            b.append("s");
            b.append(i);
            b.append(f("(", start, end, off));
            b.append(") ");
        }

        log.info("Shuffle: " + b);

        b = new StringBuilder();

        for (int i = 0; i < rdcTasks; i++) {
            int start = IDX_RESERVED_OFF  + 3 * mapTasks + i;
            int end = start + rdcTasks;

            b.append("r");
            b.append(i);
            b.append(f("(", start, end, off));
            b.append(") ");
        }

        log.info("Reduce: " + b);

        log.info(f("Cleanup: ", IDX_CLEANUP_START, IDX_CLEANUP_END, off));
    }

    private String f(String p, int idx1, int idx2, long off) {
        if (arr.get(idx1) == 0)
            return "";

        long start = (arr.get(idx1) - off);
        long end = (arr.get(idx2) - off);

        return p + start + " - " + end;
    }
}

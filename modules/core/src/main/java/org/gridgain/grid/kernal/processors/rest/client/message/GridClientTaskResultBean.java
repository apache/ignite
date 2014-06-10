/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest.client.message;

import org.gridgain.grid.portable.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

/**
 * Task result.
 */
public class GridClientTaskResultBean implements Externalizable, GridPortableEx {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final int PORTABLE_TYPE_ID = GridClientAbstractMessage.nextSystemTypeId();

    /** Synthetic ID containing task ID and result holding node ID. */
    private String id;

    /** Execution finished flag. */
    private boolean finished;

    /** Result. */
    private Object res;

    /** Error if any occurs while execution. */
    private String error;

    /**
     * @return Task ID.
     */
    public String getId() {
        return id;
    }

    /**
     * @param id Task ID.
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * @return {@code true} if execution finished.
     */
    public boolean isFinished() {
        return finished;
    }

    /**
     * @param finished {@code true} if execution finished.
     */
    public void setFinished(boolean finished) {
        this.finished = finished;
    }

    /**
     * @return Task result.
     */
    @SuppressWarnings("unchecked")
    public <R> R getResult() {
        return (R)res;
    }

    /**
     * @param res Task result.
     */
    public void setResult(Object res) {
        this.res = res;
    }

    /**
     * @return Error.
     */
    public String getError() {
        return error;
    }

    /**
     * @param error Error.
     */
    public void setError(String error) {
        this.error = error;
    }

    /** {@inheritDoc} */
    @Override public int typeId() {
        return PORTABLE_TYPE_ID;
    }

    /** {@inheritDoc} */
    @Override public void writePortable(GridPortableWriter writer) throws IOException {
        writer.writeString("id", id);
        writer.writeBoolean("finished", finished);
        writer.writeObject("res", res);
        writer.writeString("error", error);
    }

    /** {@inheritDoc} */
    @Override public void readPortable(GridPortableReader reader) throws IOException {
        id = reader.readString("id");
        finished = reader.readBoolean("finished");
        res = reader.readObject("res");
        error = reader.readString("error");
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeString(out, id);
        out.writeBoolean(finished);
        out.writeObject(res);
        U.writeString(out, error);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        id = U.readString(in);
        finished = in.readBoolean();
        res = in.readObject();
        error = U.readString(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return getClass().getSimpleName() + " [res=" + res + ", error=" + error +
            ", finished=" + finished + ", id=" + id + "]";
    }
}

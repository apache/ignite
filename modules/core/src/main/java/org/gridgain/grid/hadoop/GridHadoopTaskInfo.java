/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.hadoop;

import java.io.*;
import java.util.*;

/**
 * TODO write doc
 */
public class GridHadoopTaskInfo implements Externalizable {
    /** */
    private UUID nodeId;

    /** */
    private GridHadoopTaskType type;

    /** */
    private GridHadoopJobId jobId;

    /** */
    private int taskNumber;

    /** */
    private int attempt;

    /** */
    private GridHadoopFileBlock fileBlock;

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {

    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

    }
}

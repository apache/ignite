/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.hadoop;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.hadoop.v2.*;
import org.gridgain.grid.logger.*;
import org.jetbrains.annotations.*;

import java.io.*;

/**
 * Hadoop job info based on default Hadoop configuration.
 */
public class GridHadoopDefaultJobInfo implements GridHadoopJobInfo, Externalizable {
    /** Old mapper class attribute. */
    private static final String OLD_MAP_CLASS_ATTR = "mapred.mapper.class";

    /** Old reducer class attribute. */
    private static final String OLD_REDUCE_CLASS_ATTR = "mapred.reducer.class";

    /** Configuration. */
    private JobConf cfg;

    /**
     * Default constructor required by {@link Externalizable}.
     */
    public GridHadoopDefaultJobInfo() {
        // No-op.
    }

    /**
     * @param cfg Hadoop configuration.
     */
    public GridHadoopDefaultJobInfo(Configuration cfg) throws GridException {
        this.cfg = new JobConf(cfg);

        setUseNewAPI();
    }

    /**
     * Checks the attribute in configuration is not set.
     *
     * @param attr Attribute name.
     * @param msg Message for creation of exception.
     * @throws GridException If attribute is set.
     */
    private void ensureNotSet(String attr, String msg) throws GridException {
        if (cfg.get(attr) != null)
            throw new GridException(attr + " is incompatible with " + msg + " mode.");
    }

    /**
     * Default to the new APIs unless they are explicitly set or the old mapper or reduce attributes are used.
     *
     * @throws GridException If the configuration is inconsistent.
     */
    private void setUseNewAPI() throws GridException {
        int numReduces = cfg.getNumReduceTasks();

        cfg.setBooleanIfUnset("mapred.mapper.new-api", cfg.get(OLD_MAP_CLASS_ATTR) == null);

        if (cfg.getUseNewMapper()) {
            String mode = "new map API";

            ensureNotSet("mapred.input.format.class", mode);
            ensureNotSet(OLD_MAP_CLASS_ATTR, mode);

            if (numReduces != 0)
                ensureNotSet("mapred.partitioner.class", mode);
            else
                ensureNotSet("mapred.output.format.class", mode);
        }
        else {
            String mode = "map compatibility";

            ensureNotSet(MRJobConfig.INPUT_FORMAT_CLASS_ATTR, mode);
            ensureNotSet(MRJobConfig.MAP_CLASS_ATTR, mode);

            if (numReduces != 0)
                ensureNotSet(MRJobConfig.PARTITIONER_CLASS_ATTR, mode);
            else
                ensureNotSet(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR, mode);
        }

        if (numReduces != 0) {
            cfg.setBooleanIfUnset("mapred.reducer.new-api", cfg.get(OLD_REDUCE_CLASS_ATTR) == null);

            if (cfg.getUseNewReducer()) {
                String mode = "new reduce API";

                ensureNotSet("mapred.output.format.class", mode);
                ensureNotSet(OLD_REDUCE_CLASS_ATTR, mode);
            }
            else {
                String mode = "reduce compatibility";

                ensureNotSet(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR, mode);
                ensureNotSet(MRJobConfig.REDUCE_CLASS_ATTR, mode);
            }
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public String property(String name) {
        return cfg.get(name);
    }

    /** {@inheritDoc} */
    @Override public GridHadoopJob createJob(GridHadoopJobId jobId, GridLogger log) {
        return new GridHadoopV2Job(jobId, this, log);
    }

    /** {@inheritDoc} */
    @Override public boolean hasCombiner() {
        return cfg.get("mapred.combiner.class") != null ||
            cfg.get(MRJobConfig.COMBINE_CLASS_ATTR) != null;
    }

    /** {@inheritDoc} */
    @Override public boolean hasReducer() {
        return reducers() > 0;
    }

    /**
     * @return Number of reducers configured for job.
     */
    public int reducers() {
        return cfg.getNumReduceTasks();
    }

    /**
     * @return Hadoop configuration.
     */
    public JobConf configuration() {
        return cfg;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        cfg.write(out);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        cfg = new JobConf();

        cfg.readFields(in);
    }
}

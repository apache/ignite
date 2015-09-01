/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.hadoop;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Hadoop job info based on default Hadoop configuration.
 */
public class HadoopDefaultJobInfo implements HadoopJobInfo, Externalizable {
    /** */
    private static final long serialVersionUID = 5489900236464999951L;

    /** {@code true} If job has combiner. */
    private boolean hasCombiner;

    /** Number of reducers configured for job. */
    private int numReduces;

    /** Configuration. */
    private Map<String,String> props = new HashMap<>();

    /** Job name. */
    private String jobName;

    /** User name. */
    private String user;

    /**
     * Default constructor required by {@link Externalizable}.
     */
    public HadoopDefaultJobInfo() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param jobName Job name.
     * @param user User name.
     * @param hasCombiner {@code true} If job has combiner.
     * @param numReduces Number of reducers configured for job.
     * @param props All other properties of the job.
     */
    public HadoopDefaultJobInfo(String jobName, String user, boolean hasCombiner, int numReduces,
        Map<String, String> props) {
        this.jobName = jobName;
        this.user = user;
        this.hasCombiner = hasCombiner;
        this.numReduces = numReduces;
        this.props = props;
    }

    /** {@inheritDoc} */
    @Nullable @Override public String property(String name) {
        return props.get(name);
    }

    /** {@inheritDoc} */
    @Override public HadoopJob createJob(Class<? extends HadoopJob> jobCls,
            HadoopJobId jobId, IgniteLogger log) throws IgniteCheckedException {
        assert jobCls != null;

        try {
            Constructor<? extends HadoopJob> constructor = jobCls.getConstructor(HadoopJobId.class,
                HadoopDefaultJobInfo.class, IgniteLogger.class);

            return constructor.newInstance(jobId, this, log);
        }
        // NB: java.lang.NoClassDefFoundError may be thrown from Class#getConstructor() call.
        catch (Throwable t) {
            if (t instanceof Error)
                throw (Error)t;
            
            throw new IgniteCheckedException(t);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean hasCombiner() {
        return hasCombiner;
    }

    /** {@inheritDoc} */
    @Override public boolean hasReducer() {
        return reducers() > 0;
    }

    /** {@inheritDoc} */
    @Override public int reducers() {
        return numReduces;
    }

    /** {@inheritDoc} */
    @Override public String jobName() {
        return jobName;
    }

    /** {@inheritDoc} */
    @Override public String user() {
        return user;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeString(out, jobName);
        U.writeString(out, user);

        out.writeBoolean(hasCombiner);
        out.writeInt(numReduces);

        U.writeStringMap(out, props);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        jobName = U.readString(in);
        user = U.readString(in);

        hasCombiner = in.readBoolean();
        numReduces = in.readInt();

        props = U.readStringMap(in);
    }

    /**
     * @return Properties of the job.
     */
    public Map<String, String> properties() {
        return props;
    }
}
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

package org.apache.ignite.spi.deployment.uri;

import java.io.File;
import java.util.List;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Deployment file processing result.
 */
class GridUriDeploymentFileProcessorResult {
    /** Class loader. */
    private ClassLoader clsLdr;

    /** Task class. */
    private List<Class<? extends ComputeTask<?, ?>>> taskClss;

    /** Deploument unit file. */
    private File file;

    /** Hash of deployment unit. */
    private String md5;

    /**
     * Getter for property 'clsLdr'.
     *
     * @return Value for property 'clsLdr'.
     */
    public ClassLoader getClassLoader() {
        return clsLdr;
    }

    /**
     * Setter for property 'clsLdr'.
     *
     * @param clsLdr Value to set for property 'clsLdr'.
     */
    public void setClassLoader(ClassLoader clsLdr) {
        this.clsLdr = clsLdr;
    }

    /**
     * Getter for property 'taskClss'.
     *
     * @return Value for property 'taskClss'.
     */
    public List<Class<? extends ComputeTask<?, ?>>> getTaskClasses() {
        return taskClss;
    }

    /**
     * Setter for property 'taskClss'.
     *
     * @param taskClss Value to set for property 'taskClss'.
     */
    public void setTaskClasses(List<Class<? extends ComputeTask<?, ?>>> taskClss) {
        this.taskClss = taskClss;
    }

    /**
     * Gets GAR file.
     *
     * @return GAR file.
     */
    public File getFile() {
        return file;
    }

    /**
     * Sets GAR file.
     *
     * @param file GAR file.
     */
    public void setFile(File file) {
        this.file = file;
    }

    /**
     * @return md5 hash of this deployment result.
     */
    public String getMd5() {
        return md5;
    }

    /**
     * @param md5 md5 hash of this deployment result.
     */
    public void setMd5(String md5) {
        this.md5 = md5;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridUriDeploymentFileProcessorResult.class, this);
    }
}
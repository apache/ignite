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

package org.apache.ignite.internal.processors.hadoop.v2;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

/**
 * A fake helper to load the native hadoop code i.e. libhadoop.so.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class HadoopNativeCodeLoader {
    /**
     * Check if native-hadoop code is loaded for this platform.
     *
     * @return <code>true</code> if native-hadoop is loaded,
     *         else <code>false</code>
     */
    public static boolean isNativeCodeLoaded() {
        return false;
    }

    /**
     * Returns true only if this build was compiled with support for snappy.
     */
    public static boolean buildSupportsSnappy() {
        return false;
    }

    /**
     * @return Library name.
     */
    public static String getLibraryName() {
        throw new IllegalStateException();
    }

    /**
     * Return if native hadoop libraries, if present, can be used for this job.
     * @param conf configuration
     *
     * @return <code>true</code> if native hadoop libraries, if present, can be
     *         used for this job; <code>false</code> otherwise.
     */
    public boolean getLoadNativeLibraries(Configuration conf) {
        return false;
    }

    /**
     * Set if native hadoop libraries, if present, can be used for this job.
     *
     * @param conf configuration
     * @param loadNativeLibraries can native hadoop libraries be loaded
     */
    public void setLoadNativeLibraries(Configuration conf, boolean loadNativeLibraries) {
        // No-op.
    }
}

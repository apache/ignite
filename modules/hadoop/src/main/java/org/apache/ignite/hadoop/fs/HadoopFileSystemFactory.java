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

package org.apache.ignite.hadoop.fs;

import java.io.IOException;
import java.io.Serializable;
import org.apache.hadoop.fs.FileSystem;

/**
 * This factory is {@link Serializable} because it should be transferable over the network.
 * <p>
 * Implementations may choose not to construct a new instance, but instead
 * return a previously created instance.
 */
public interface HadoopFileSystemFactory extends Serializable {
    /**
     * Creates the file system, possibly taking a cached instance.
     * All the other data needed for the file system creation are expected to be contained
     * in this object instance.
     *
     * @param userName The user name
     * @return The file system.
     * @throws IOException On error.
     */
    public FileSystem create(String userName) throws IOException;
}

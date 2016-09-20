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

import org.apache.ignite.hadoop.util.KerberosUserNameMapper;
import org.apache.ignite.hadoop.util.UserNameMapper;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Simple Hadoop file system factory which delegates to {@code FileSystem.get()} on each call.
 * <p>
 * If {@code "fs.[prefix].impl.disable.cache"} is set to {@code true}, file system instances will be cached by Hadoop.
 */
public class BasicHadoopFileSystemFactory implements HadoopFileSystemFactory, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

        /** File system URI. */
    private String uri;

    /** File system config paths. */
    private String[] cfgPaths;

    /** User name mapper. */
    private UserNameMapper usrNameMapper;

    /**
     * Constructor.
     */
    public BasicHadoopFileSystemFactory() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public final Object get(String name) throws IOException {
        throw new UnsupportedOperationException("Method should not be called directly.");
    }

    /**
     * Gets file system URI.
     * <p>
     * This URI will be used as a first argument when calling {@code FileSystem.get(URI, Configuration, String)}.
     * <p>
     * If not set, default URI will be picked from file system configuration using
     * {@code FileSystem.getDefaultUri(Configuration)} method.
     *
     * @return File system URI.
     */
    @Nullable public String getUri() {
        return uri;
    }

    /**
     * Sets file system URI. See {@link #getUri()} for more information.
     *
     * @param uri File system URI.
     */
    public void setUri(@Nullable String uri) {
        this.uri = uri;
    }

    /**
     * Gets paths to additional file system configuration files (e.g. core-site.xml).
     * <p>
     * Path could be either absolute or relative to {@code IGNITE_HOME} environment variable.
     * <p>
     * All provided paths will be loaded in the order they provided and then applied to {@code Configuration}. It means
     * that path order might be important in some cases.
     *
     * @return Paths to file system configuration files.
     */
    @Nullable public String[] getConfigPaths() {
        return cfgPaths;
    }

    /**
     * Set paths to additional file system configuration files (e.g. core-site.xml). See {@link #getConfigPaths()} for
     * more information.
     *
     * @param cfgPaths Paths to file system configuration files.
     */
    public void setConfigPaths(@Nullable String... cfgPaths) {
        this.cfgPaths = cfgPaths;
    }

    /**
     * Get optional user name mapper.
     * <p>
     * When IGFS is invoked from Hadoop, user name is passed along the way to ensure that request will be performed
     * with proper user context. User name is passed in a simple form and doesn't contain any extended information,
     * such as host, domain or Kerberos realm. You may use name mapper to translate plain user name to full user
     * name required by security engine of the underlying file system.
     * <p>
     * For example you may want to use {@link KerberosUserNameMapper} to user name from {@code "johndoe"} to
     * {@code "johndoe@YOUR.REALM.COM"}.
     *
     * @return User name mapper.
     */
    @Nullable public UserNameMapper getUserNameMapper() {
        return usrNameMapper;
    }

    /**
     * Set optional user name mapper. See {@link #getUserNameMapper()} for more information.
     *
     * @param usrNameMapper User name mapper.
     */
    public void setUserNameMapper(@Nullable UserNameMapper usrNameMapper) {
        this.usrNameMapper = usrNameMapper;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeString(out, uri);

        if (cfgPaths != null) {
            out.writeInt(cfgPaths.length);

            for (String cfgPath : cfgPaths)
                U.writeString(out, cfgPath);
        }
        else
            out.writeInt(-1);

        out.writeObject(usrNameMapper);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        uri = U.readString(in);

        int cfgPathsCnt = in.readInt();

        if (cfgPathsCnt != -1) {
            cfgPaths = new String[cfgPathsCnt];

            for (int i = 0; i < cfgPathsCnt; i++)
                cfgPaths[i] = U.readString(in);
        }

        usrNameMapper = (UserNameMapper)in.readObject();
    }
}

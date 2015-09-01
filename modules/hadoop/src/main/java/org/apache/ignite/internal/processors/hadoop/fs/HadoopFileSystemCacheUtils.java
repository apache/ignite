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

package org.apache.ignite.internal.processors.hadoop.fs;

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.ignite.IgniteException;
import org.apache.ignite.hadoop.fs.v1.IgniteHadoopFileSystem;
import org.apache.ignite.internal.util.GridStringBuilder;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

/**
 * File system cache utility methods used by Map-Reduce tasks and jobs.
 */
public class HadoopFileSystemCacheUtils {
    /**
     * A common static factory method. Creates new HadoopLazyConcurrentMap.
     * @return a new HadoopLazyConcurrentMap.
     */
    public static HadoopLazyConcurrentMap<FsCacheKey, FileSystem> createHadoopLazyConcurrentMap() {
        return new HadoopLazyConcurrentMap<>(
            new HadoopLazyConcurrentMap.ValueFactory<FsCacheKey, FileSystem>() {
                @Override public FileSystem createValue(FsCacheKey key) {
                    try {
                        assert key != null;

                        // Explicitly disable FileSystem caching:
                        URI uri = key.uri();

                        String scheme = uri.getScheme();

                        // Copy the configuration to avoid altering the external object.
                        Configuration cfg = new Configuration(key.configuration());

                        String prop = HadoopFileSystemsUtils.disableFsCachePropertyName(scheme);

                        cfg.setBoolean(prop, true);

                        return FileSystem.get(uri, cfg, key.user());
                    }
                    catch (IOException | InterruptedException ioe) {
                        throw new IgniteException(ioe);
                    }
                }
            }
        );
    }

    /**
     * Gets non-null user name as per the Hadoop viewpoint.
     * @param cfg the Hadoop job configuration, may be null.
     * @return the user name, never null.
     */
    private static String getMrHadoopUser(Configuration cfg) throws IOException {
        String user = cfg.get(MRJobConfig.USER_NAME);

        if (user == null)
            user = IgniteHadoopFileSystem.getFsHadoopUser();

        return user;
    }

    /**
     * Common method to get the V1 file system in MapRed engine.
     * It gets the filesystem for the user specified in the
     * configuration with {@link MRJobConfig#USER_NAME} property.
     * The file systems are created and cached in the given map upon first request.
     *
     * @param uri The file system uri.
     * @param cfg The configuration.
     * @param map The caching map.
     * @return The file system.
     * @throws IOException On error.
     */
    public static FileSystem fileSystemForMrUserWithCaching(@Nullable URI uri, Configuration cfg,
        HadoopLazyConcurrentMap<FsCacheKey, FileSystem> map)
            throws IOException {
        assert map != null;
        assert cfg != null;

        final String usr = getMrHadoopUser(cfg);

        assert usr != null;

        if (uri == null)
            uri = FileSystem.getDefaultUri(cfg);

        final FileSystem fs;

        try {
            final FsCacheKey key = new FsCacheKey(uri, usr, cfg);

            fs = map.getOrCreate(key);
        }
        catch (IgniteException ie) {
            throw new IOException(ie);
        }

        assert fs != null;
        assert !(fs instanceof IgniteHadoopFileSystem) || F.eq(usr, ((IgniteHadoopFileSystem)fs).user());

        return fs;
    }

    /**
     * Takes Fs URI using logic similar to that used in FileSystem#get(1,2,3).
     * @param uri0 The uri.
     * @param cfg The cfg.
     * @return Correct URI.
     */
    private static URI fixUri(URI uri0, Configuration cfg) {
        if (uri0 == null)
            return FileSystem.getDefaultUri(cfg);

        String scheme = uri0.getScheme();
        String authority = uri0.getAuthority();

        if (authority == null) {
            URI dfltUri = FileSystem.getDefaultUri(cfg);

            if (scheme == null || (scheme.equals(dfltUri.getScheme()) && dfltUri.getAuthority() != null))
                return dfltUri;
        }

        return uri0;
    }

    /**
     * Note that configuration is not a part of the key.
     * It is used solely to initialize the first instance
     * that is created for the key.
     */
    public static final class FsCacheKey {
        /** */
        private final URI uri;

        /** */
        private final String usr;

        /** */
        private final String equalityKey;

        /** */
        private final Configuration cfg;

        /**
         * Constructor
         */
        public FsCacheKey(URI uri, String usr, Configuration cfg) {
            assert uri != null;
            assert usr != null;
            assert cfg != null;

            this.uri = fixUri(uri, cfg);
            this.usr = usr;
            this.cfg = cfg;

            this.equalityKey = createEqualityKey();
        }

        /**
         * Creates String key used for equality and hashing.
         */
        private String createEqualityKey() {
            GridStringBuilder sb = new GridStringBuilder("(").a(usr).a(")@");

            if (uri.getScheme() != null)
                sb.a(uri.getScheme().toLowerCase());

            sb.a("://");

            if (uri.getAuthority() != null)
                sb.a(uri.getAuthority().toLowerCase());

            return sb.toString();
        }

        /**
         * The URI.
         */
        public URI uri() {
            return uri;
        }

        /**
         * The User.
         */
        public String user() {
            return usr;
        }

        /**
         * The Configuration.
         */
        public Configuration configuration() {
            return cfg;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("SimplifiableIfStatement")
        @Override public boolean equals(Object obj) {
            if (obj == this)
                return true;

            if (obj == null || getClass() != obj.getClass())
                return false;

            return equalityKey.equals(((FsCacheKey)obj).equalityKey);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return equalityKey.hashCode();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return equalityKey;
        }
    }
}
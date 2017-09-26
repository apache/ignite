/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.filename;

import java.io.File;
import java.io.FilenameFilter;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Component for resolving PDS storage file names, also used for generating consistent ID for case PDS mode is enabled
 */
public class PdsCompatibleFileNameResolver {
    public static final String DB_FOLDER_PREFIX = "Node";
    public static final String NODEIDX_UID_SEPARATOR = "-";

    public static final String NODE_PATTERN = DB_FOLDER_PREFIX +
        "[0-9]*" +
        NODEIDX_UID_SEPARATOR;
    public static final String UUID_STR_PATTERN = "[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}";
    public static final String UUID_STR_MASKED_FILES_PATTERN = "[a-fA-F0-9]{8}_[a-fA-F0-9]{4}_[a-fA-F0-9]{4}_[a-fA-F0-9]{4}_[a-fA-F0-9]{12}";

    private Object consId;
    private IgniteConfiguration cfg;
    private GridDiscoveryManager discovery;
    //todo remove static
    private static PdsFolderSettings settings;

    public PdsCompatibleFileNameResolver(IgniteConfiguration cfg, GridDiscoveryManager discovery) {
        this.cfg = cfg;
        this.discovery = discovery;
    }

    public PdsFolderSettings compatibleResolve() {
        return new PdsFolderSettings(discovery.consistentId().toString(), true);
    }

    public PdsFolderSettings resolveFolders() throws IgniteCheckedException {
        if (settings == null)
            settings = prepareNewSettings();
        return settings;
    }

    private PdsFolderSettings prepareNewSettings() throws IgniteCheckedException {
        if (!cfg.isPersistentStoreEnabled())
            return compatibleResolve();

        if (consId != null)
            return new PdsFolderSettings(consId, true);

        if (consId == null) {
            if (cfg.getConsistentId() != null) {
                consId = cfg.getConsistentId(); // compatible mode from configuration is used fot this case
                return new PdsFolderSettings(consId, true);
            }
        }

        // The node scans the work directory and checks if there is a folder matching the consistent ID. If such a folder exists, we start up with this ID (compatibility mode)

        final PersistentStoreConfiguration pstCfg = cfg.getPersistentStoreConfiguration();

        final File pstStoreBasePath = resolvePersistentStoreBasePath(pstCfg);
        final FilenameFilter filter = new FilenameFilter() {
            @Override public boolean accept(File dir, String name) {
                return name.matches(NODE_PATTERN + UUID_STR_PATTERN);
            }
        };

        final String[] newStyleFiles = pstStoreBasePath.list(filter);
        for (String file : newStyleFiles) {
            Matcher m = Pattern.compile(NODE_PATTERN).matcher(file);
            if (m.find()) {
                int uidStart = m.end();
                String uid = file.substring(uidStart);
                System.out.println("found>> " + uid);

                int idx = 0; //todo
                //already have such directory here
                return new PdsFolderSettings(UUID.fromString(uid), file, idx, false);
            }
        }

        UUID uuid = UUID.randomUUID();
        String uuidAsStr = uuid.toString();

        assert uuidAsStr.matches(UUID_STR_PATTERN);
        int nodeIdx = 0;

        String consIdFolderReplacement = DB_FOLDER_PREFIX + Integer.toString(nodeIdx) + NODEIDX_UID_SEPARATOR + uuidAsStr;

        cfg.setConsistentId(uuidAsStr);
        return new PdsFolderSettings(uuid, consIdFolderReplacement, nodeIdx, false);
    }

    private File resolvePersistentStoreBasePath(PersistentStoreConfiguration pstCfg) throws IgniteCheckedException {
        final File dirToFindOldConsIds;
        if (pstCfg.getPersistentStorePath() != null) {
            File workDir0 = new File(pstCfg.getPersistentStorePath());

            if (!workDir0.isAbsolute())
                dirToFindOldConsIds = U.resolveWorkDirectory(
                    cfg.getWorkDirectory(),
                    pstCfg.getPersistentStorePath(),
                    false
                );
            else
                dirToFindOldConsIds = workDir0;
        }
        else {
            dirToFindOldConsIds = U.resolveWorkDirectory(
                cfg.getWorkDirectory(),
                "db",
                false
            );
        }
        return dirToFindOldConsIds;
    }

}

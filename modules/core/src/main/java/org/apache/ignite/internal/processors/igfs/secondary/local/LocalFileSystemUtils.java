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

package org.apache.ignite.internal.processors.igfs.secondary.local;

import org.apache.ignite.igfs.IgfsException;
import org.apache.ignite.internal.processors.igfs.IgfsUtils;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Various utility methods for local file system.
 */
public class LocalFileSystemUtils {
    /** Posix file permissions. */
    public static final PosixFilePermission[] POSIX_PERMISSIONS = PosixFilePermission.values();

    /**
     * Update file properties.
     *
     * @param file File.
     * @param grp Group.
     * @param perm Permissions.
     */
    public static void updateProperties(File file, String grp, String perm) {
        PosixFileAttributeView attrs = Files.getFileAttributeView(file.toPath(), PosixFileAttributeView.class);

        if (attrs == null)
            throw new UnsupportedOperationException("Posix file attributes not available");

        if (grp != null) {
            try {
                UserPrincipalLookupService lookupService = FileSystems.getDefault().getUserPrincipalLookupService();

                GroupPrincipal grp0 = lookupService.lookupPrincipalByGroupName(grp);

                attrs.setGroup(grp0);
            }
            catch (IOException e) {
                throw new IgfsException("Update the '" + IgfsUtils.PROP_GROUP_NAME + "' property is failed.", e);
            }
        }

        if (perm != null) {
            int perm0 = Integer.parseInt(perm, 8);

            Set<PosixFilePermission> permSet = new HashSet<>(9);

            for (int i = 0; i < LocalFileSystemUtils.POSIX_PERMISSIONS.length; ++i) {
                if ((perm0 & (1 << i)) != 0)
                    permSet.add(LocalFileSystemUtils.POSIX_PERMISSIONS[i]);
            }

            try {
                attrs.setPermissions(permSet);
            }
            catch (IOException e) {
                throw new IgfsException("Update the '" + IgfsUtils.PROP_PERMISSION + "' property is failed.", e);
            }
        }
    }

    /**
     * Get POSIX attributes for file.
     *
     * @param file File.
     */
    @Nullable public static PosixFileAttributes posixAttributes(File file) {
        PosixFileAttributes attrs = null;

        try {
            PosixFileAttributeView view = Files.getFileAttributeView(file.toPath(), PosixFileAttributeView.class);

            if (view != null)
                attrs = view.readAttributes();
        }
        catch (IOException e) {
            throw new IgfsException("Failed to read POSIX attributes: " + file.getAbsolutePath(), e);
        }

        return attrs;
    }

    /**
     * Convert POSIX attributes to property map.
     *
     * @param attrs Attributes view.
     * @return IGFS properties map.
     */
    public static Map<String, String> posixAttributesToMap(PosixFileAttributes attrs) {
        if (attrs == null)
            return null;

        Map<String, String> props = U.newHashMap(3);

        props.put(IgfsUtils.PROP_USER_NAME, attrs.owner().getName());
        props.put(IgfsUtils.PROP_GROUP_NAME, attrs.group().getName());

        int perm = 0;

        for(PosixFilePermission p : attrs.permissions())
            perm |= (1 << 8 - p.ordinal());

        props.put(IgfsUtils.PROP_PERMISSION, '0' + Integer.toOctalString(perm));

        return props;
    }

    /**
     * Private constructor.
     */
    private LocalFileSystemUtils() {
        // No-op.
    }
}

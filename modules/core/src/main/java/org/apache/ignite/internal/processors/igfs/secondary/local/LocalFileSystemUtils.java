/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.igfs.secondary.local;

import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
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
     * @return PosixFileAttributes.
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
     * Get POSIX attributes for file.
     *
     * @param file File.
     * @return BasicFileAttributes.
     */
    @Nullable public static BasicFileAttributes basicAttributes(File file) {
        BasicFileAttributes attrs = null;

        try {
            BasicFileAttributeView view = Files.getFileAttributeView(file.toPath(), BasicFileAttributeView.class);

            if (view != null)
                attrs = view.readAttributes();
        }
        catch (IOException e) {
            throw new IgfsException("Failed to read basic file attributes: " + file.getAbsolutePath(), e);
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

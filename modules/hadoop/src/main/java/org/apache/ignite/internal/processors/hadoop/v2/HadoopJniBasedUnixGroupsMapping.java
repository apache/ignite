package org.apache.ignite.internal.processors.hadoop.v2;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.GroupMappingServiceProvider;
import org.apache.hadoop.security.JniBasedUnixGroupsMapping;

/**
 * A JNI-based implementation of {@link GroupMappingServiceProvider}
 * that invokes libC calls to get the group
 * memberships of a given user.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class HadoopJniBasedUnixGroupsMapping implements GroupMappingServiceProvider {

    private static final Log LOG =
        LogFactory.getLog(JniBasedUnixGroupsMapping.class);

    static {
        if (!HadoopNativeCodeLoader.isNativeCodeLoaded()) {
            throw new RuntimeException("Bailing out since native library couldn't " +
                "be loaded");
        }
        anchorNative();
        LOG.debug("Using JniBasedUnixGroupsMapping for Group resolution");
    }

    /**
     * Set up our JNI resources.
     *
     * @throws                 RuntimeException if setup fails.
     */
    static void anchorNative() {
        try {
            ClassLoader parent = Configuration.class.getClassLoader().getParent();

            Class cls = Class.forName(JniBasedUnixGroupsMapping.class.getName(), true, parent);

            Method m = cls.getDeclaredMethod("anchorNative");

            m.setAccessible(true);

            m.invoke(null);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get the set of groups associated with a user.
     *
     * @param username           The user name
     *
     * @return                   The set of groups associated with a user.
     */
    native static String[] getGroupsForUser(String username);

    /**
     * Log an error message about a group.  Used from JNI.
     */
    static private void logError(int groupId, String error) {
        LOG.error("error looking up the name of group " + groupId + ": " + error);
    }

    @Override
    public List<String> getGroups(String user) throws IOException {
        String[] groups = new String[0];
        try {
            groups = getGroupsForUser(user);
        } catch (Exception e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Error getting groups for " + user, e);
            } else {
                LOG.info("Error getting groups for " + user + ": " + e.getMessage());
            }
        }
        return Arrays.asList(groups);
    }

    @Override
    public void cacheGroupsRefresh() throws IOException {
        // does nothing in this provider of user to groups mapping
    }

    @Override
    public void cacheGroupsAdd(List<String> groups) throws IOException {
        // does nothing in this provider of user to groups mapping
    }
}

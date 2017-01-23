package org.apache.ignite.testframework.junits.multijvm;

import java.io.File;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.Nullable;

/**
 * Structure to hold node process parameters.
 */
public class NodeProcessParameters {
    public static NodeProcessParameters DEFAULT = new NodeProcessParameters(false, false, null, null);

    public final boolean uniqueWorkDir;
    public final boolean uniqueProcessDir;
    public final Map<String, String> processEnv;
    public final List<String> jvmArguments;

    public NodeProcessParameters(boolean uniqueWorkDir, boolean uniqueProcessDir,
        @Nullable Map<String, String> processEnv, @Nullable List<String> jvmArguments) {
        this.uniqueWorkDir = uniqueWorkDir;
        this.uniqueProcessDir = uniqueProcessDir;
        this.processEnv = processEnv;
        this.jvmArguments = jvmArguments;
    }
}

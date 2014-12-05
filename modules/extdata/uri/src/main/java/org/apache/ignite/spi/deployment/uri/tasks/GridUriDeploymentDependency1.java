/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi.deployment.uri.tasks;

import org.gridgain.grid.util.typedef.internal.*;
import java.io.*;
import java.util.*;

/**
 * This class used by {@link GridUriDeploymentTestTask2} which loaded from GAR file.
 * GridDependency loaded from {@code /lib/*.jar} in GAR file.
 * GridDependency load resource {@code test.properties} from the same jar in {@code /lib/*.jar}
 */
public class GridUriDeploymentDependency1 {
    /** */
    public static final String RESOURCE = "org/gridgain/grid/spi/deployment/uri/tasks/test1.properties";

    /**
     * @return Value of the property {@code test1.txt} loaded from the {@code test1.properties} file.
     */
    public String getMessage() {
        InputStream in = null;

        try {
            in = getClass().getClassLoader().getResourceAsStream(RESOURCE);

            Properties props = new Properties();

            props.load(in);

            return props.getProperty("test1.txt");
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        finally{
            U.close(in, null);
        }

        return null;
    }
}

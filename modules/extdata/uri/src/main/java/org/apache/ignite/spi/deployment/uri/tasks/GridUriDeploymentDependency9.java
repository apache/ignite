/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.spi.deployment.uri.tasks;

import org.apache.ignite.internal.util.typedef.internal.U;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * This class is used by {@link GridUriDeploymentTestTask9} which is loaded from a JAR file.
 * It's loaded along with {@code test9.properties} file from the same JAR.
 */
public class GridUriDeploymentDependency9 {
    /** */
    public static final String RESOURCE = "org/apache/ignite/spi/deployment/uri/tasks/test9.properties";

    /**
     * @return Value of the property {@code test9.txt} loaded from the {@code test9.properties} file.
     */
    public String getMessage() {
        InputStream in = null;

        try {
            in = getClass().getClassLoader().getResourceAsStream(RESOURCE);

            Properties props = new Properties();

            props.load(in);

            return props.getProperty("test9.txt");
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

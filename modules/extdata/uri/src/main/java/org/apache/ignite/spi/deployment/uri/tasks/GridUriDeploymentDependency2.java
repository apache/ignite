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

package org.apache.ignite.spi.deployment.uri.tasks;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * This class used by {@link GridUriDeploymentTestTask2} which loaded from GAR file.
 * GridDependency loaded from {@code /lib/*.jar} in GAR file.
 * GridDependency load resource {@code test.properties} from the same jar in {@code /lib/*.jar}
 */
public class GridUriDeploymentDependency2 {
    /** */
    public static final String RESOURCE = "org/apache/ignite/grid/spi/deployment/uri/tasks/test2.properties";

    /**
     * @return Value of the property {@code test1.txt} loaded from the {@code test2.properties} file.
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
package org.apache.ignite.spi.deployment.uri.tasks;
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

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Imported class which should be placed in JAR file in GAR/lib folder.
 * Loads message resource file via class loader.
 */
public class GarHelloWorldBean {
    /** */
    public static final String RESOURCE = "gar-example.properties";

    /**
     * Gets keyed message.
     *
     * @param key Message key.
     * @return Keyed message.
     */
    @Nullable public String getMessage(String key) {
        InputStream in = null;

        try {
            in = getClass().getClassLoader().getResourceAsStream(RESOURCE);

            Properties props = new Properties();

            props.load(in);

            return props.getProperty(key);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        finally {
            U.close(in, null);
        }

        return null;
    }
}
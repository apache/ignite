package org.apache.ignite.spi.deployment.uri.tasks;
/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
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
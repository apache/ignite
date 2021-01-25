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

package org.apache.ignite.cli.builtins;

import java.io.File;
import java.net.URISyntaxException;
import java.nio.file.Path;
import javax.inject.Singleton;
import io.micronaut.core.annotation.Introspected;
import org.apache.ignite.cli.IgniteCLIException;
import org.apache.ignite.cli.IgniteCliApp;

/**
 * Interface for resolving different fs paths like home directory.
 */
public interface SystemPathResolver {
    /**
     * @return System specific user home directory.
     */
    Path osHomeDirectoryPath();

    /**
     * @return Directory where CLI tool binary placed.
     */
    Path toolHomeDirectoryPath();

    /**
     *
     */
    @Singleton
    @Introspected
    class DefaultPathResolver implements SystemPathResolver {
        /** {@inheritDoc} */
        @Override public Path osHomeDirectoryPath() {
            return Path.of(System.getProperty("user.home"));
        }

        /** {@inheritDoc} */
        @Override public Path toolHomeDirectoryPath() {
            try {
                var file = new File(IgniteCliApp.class.getProtectionDomain().getCodeSource().getLocation().toURI());

                while (!file.isDirectory())
                    file = file.getParentFile();

                return file.toPath();
            }
            catch (URISyntaxException e) {
                throw new IgniteCLIException("Failed to resolve the CLI tool home directory.", e);
            }
        }
    }
}

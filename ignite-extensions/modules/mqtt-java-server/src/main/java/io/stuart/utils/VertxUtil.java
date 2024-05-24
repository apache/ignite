/*
 * Copyright 2019 Yang Wang
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.stuart.utils;

import io.stuart.config.Config;
import io.stuart.consts.ParamConst;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.VertxOptions;
import io.vertx.core.file.FileSystemOptions;

public class VertxUtil {

    public static VertxOptions vertxOptions() {
        // initialize vert.x options
        VertxOptions vertxOpts = new VertxOptions();

        // set worker pool size
        if (Config.getVertxWorkerPoolSize() <= 0) {
            vertxOpts.setWorkerPoolSize(vertxOpts.getEventLoopPoolSize() * 2);
        } else {
            vertxOpts.setWorkerPoolSize(Config.getVertxWorkerPoolSize());
        }

        // initialize file system options
        FileSystemOptions fileSystemOpts = new FileSystemOptions();
        // set enable or disable the file cache
        fileSystemOpts.setFileCachingEnabled(Config.isVertxFileCachingEnabled());
        // set file system options
        vertxOpts.setFileSystemOptions(fileSystemOpts);

        // return vert.x options
        return vertxOpts;
    }

    public static DeploymentOptions vertxDeploymentOptions(VertxOptions vertxOpts, DeploymentOptions deploymentOptions) {
        DeploymentOptions deploymentOpts = null;

        if (deploymentOptions == null) {
            deploymentOpts = new DeploymentOptions();
        } else {
            deploymentOpts = deploymentOptions;
        }

        if (Config.isVertxMultiInstancesEnable()) {
            // get instances
            int instances = Config.getVertxMultiInstances();
            // get event loop pool size
            int eventLoopPoolSize = vertxOpts.getEventLoopPoolSize();

            if (instances <= 0 || instances >= eventLoopPoolSize) {
                instances = eventLoopPoolSize;
            }

            deploymentOpts.setInstances(instances);
        } else {
            deploymentOpts.setInstances(ParamConst.VERTX_MULTI_INSTANCES);
        }

        return deploymentOpts;
    }

}

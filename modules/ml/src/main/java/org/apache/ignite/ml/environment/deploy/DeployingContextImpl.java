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

package org.apache.ignite.ml.environment.deploy;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of {@link DeployingContext} class.
 */
public class DeployingContextImpl implements DeployingContext {
    /** Logger. */
    private static final Logger logger = LoggerFactory.getLogger(DeployingContextImpl.class);

    /** Preprocessor class. */
    private transient Class<?> preprocessorClass;

    /** Client class loader. */
    private transient ClassLoader clientClassLoader;

    /** {@inheritDoc} */
    @Override public Class<?> userClass() {
        return preprocessorClass == null ? this.getClass() : preprocessorClass;
    }

    /** {@inheritDoc} */
    @Override public ClassLoader clientClassLoader() {
        return clientClassLoader == null ? this.getClass().getClassLoader() : clientClassLoader;
    }

    /** {@inheritDoc} */
    @Override public void initByClientObject(Object jobObj) {
        if (jobObj == null) {
            logger.warn("Attempt to initialize deploy context by null");
            return;
        }

        if (preprocessorClass != null)
            logger.warn("Reinitialize deploying context [class=" + jobObj.getClass().getName() + "]");

        Object objectToDeploy = jobObj;
        while (objectToDeploy instanceof DeployableObject) {
            // TODO: GG-19105
            List<Object> deps = ((DeployableObject)objectToDeploy).getDependencies();
            if (deps.isEmpty())
                break;
            else
                objectToDeploy = deps.get(0);
        }

        assert objectToDeploy != null;
        preprocessorClass = objectToDeploy.getClass();
        clientClassLoader = preprocessorClass.getClassLoader();
    }

    /** {@inheritDoc} */
    @Override public void init(DeployingContext other) {
        this.clientClassLoader = other.clientClassLoader();
        this.preprocessorClass = other.userClass();
    }
}

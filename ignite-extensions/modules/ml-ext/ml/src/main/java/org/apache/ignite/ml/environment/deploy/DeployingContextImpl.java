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

package org.apache.ignite.ml.environment.deploy;

import java.io.Serializable;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of {@link DeployingContext} class.
 */
public class DeployingContextImpl implements DeployingContext, Serializable {
    /** Logger. */
    private static final Logger logger = LoggerFactory.getLogger(DeployingContextImpl.class);

    /** Preprocessor class. */
    private transient Class<?> preprocessorCls;

    /** Client class loader. */
    private transient ClassLoader clientClsLdr;

    /** {@inheritDoc} */
    @Override public Class<?> userClass() {
        return preprocessorCls == null ? this.getClass() : preprocessorCls;
    }

    /** {@inheritDoc} */
    @Override public ClassLoader clientClassLoader() {
        return clientClsLdr == null ? this.getClass().getClassLoader() : clientClsLdr;
    }

    /** {@inheritDoc} */
    @Override public void initByClientObject(Object jobObj) {
        if (jobObj == null) {
            logger.warn("Attempt to initialize deploy context by null");
            return;
        }

        if (preprocessorCls != null)
            logger.warn("Reinitialize deploying context [class=" + jobObj.getClass().getName() + "]");

        Object objectToDeploy = jobObj;
        while (objectToDeploy instanceof DeployableObject) {
            List<Object> deps = ((DeployableObject)objectToDeploy).getDependencies();
            if (deps.isEmpty())
                break;
            else
                objectToDeploy = deps.get(0);
        }

        assert objectToDeploy != null;
        preprocessorCls = objectToDeploy.getClass();
        clientClsLdr = preprocessorCls.getClassLoader();
    }

    /** {@inheritDoc} */
    @Override public void init(DeployingContext other) {
        this.clientClsLdr = other.clientClassLoader();
        this.preprocessorCls = other.userClass();
    }
}

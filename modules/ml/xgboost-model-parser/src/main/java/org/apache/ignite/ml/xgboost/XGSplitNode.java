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

package org.apache.ignite.ml.xgboost;

/** XGBoost model split node. */
public class XGSplitNode implements XGNode {
    /** Feature name. */
    private final String featureName;

    /** Threshold. */
    private final double threshold;

    /** "Yes" child node. */
    private XGNode yesNode;

    /** "No" child node. */
    private XGNode noNode;

    /** "Missing" child node. */
    private XGNode missingNode;

    /**
     * Constructs a new instance of XGBoost model split node.
     *
     * @param featureName Feature name.
     * @param threshold Threshold.
     */
    public XGSplitNode(String featureName, double threshold) {
        this.featureName = featureName;
        this.threshold = threshold;
    }

    /** {@inheritDoc} */
    @Override public double predict(XGObject obj) {
        Double featureVal = obj.getFeature(featureName);

        if (featureVal == null)
            return missingNode.predict(obj);
        else if (featureVal < threshold)
            return yesNode.predict(obj);
        else
            return noNode.predict(obj);
    }

    /** */
    public void setYesNode(XGNode yesNode) {
        this.yesNode = yesNode;
    }

    /** */
    public void setNoNode(XGNode noNode) {
        this.noNode = noNode;
    }

    /** */
    public void setMissingNode(XGNode missingNode) {
        this.missingNode = missingNode;
    }
}
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

package org.apache.ignite.ml.tree.ui.regression;

import java.awt.BorderLayout;
import java.awt.Color;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import javax.swing.JFrame;
import javax.swing.WindowConstants;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.dataset.impl.local.LocalDatasetBuilder;
import org.apache.ignite.ml.tree.DecisionTreeRegressor;
import org.apache.ignite.ml.tree.ui.util.ControlPanel;

public class DistributedRegressionUIApplication extends RegressionUIApplication {

    public static void main(String... args) {
        JFrame f = new JFrame();
        f.setSize(500, 650);
        f.setBackground(Color.decode("#2B2B2B"));
        f.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);

        ControlPanel ctrlPanel = new ControlPanel();
        DistributedRegressionUIApplication application = new DistributedRegressionUIApplication();
        ctrlPanel.addListener(application);

        f.add(ctrlPanel, BorderLayout.NORTH);
        f.add(application, BorderLayout.SOUTH);

        f.setVisible(true);
    }

    @Override Model<double[], Double> regress(double[][] x, double[] y, int maxDeep, double minImpurityDecrease) {
        Map<Integer, double[]> map = new HashMap<>();

        for (int i = 0; i < x.length; i++) {
            double[] row = Arrays.copyOf(x[i], x[i].length + 1);
            row[row.length - 1] = y[i];
            map.put(i, row);
        }

        LocalDatasetBuilder<Integer, double[]> datasetBuilder = new LocalDatasetBuilder<>(map, 2);

        return new DecisionTreeRegressor(maxDeep, minImpurityDecrease).fit(
            datasetBuilder,
            (k, v) -> Arrays.copyOf(v, v.length - 1),
            (k, v) -> v[v.length - 1]
        );
    }
}

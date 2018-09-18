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
package org.apache.ignite.internal.processors.cache.ratemetrics;

import java.awt.Dimension;
import java.awt.EventQueue;
import java.awt.FlowLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.Timer;
import org.apache.ignite.internal.util.IgniteUtils;

/**
 * Visualization of {@link HitRateMetrics}.
 */
public class HitRateMetricsSandbox extends JFrame {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** */
    private final JLabel rateLb = new JLabel("0.0");

    /** */
    private final HitRateMetrics metrics = new HitRateMetrics(5_000, 20);

    /**
     * Default constructor.
     */
    private HitRateMetricsSandbox() {
        IgniteUtils.onGridStart();

        JButton hitBtn = new JButton("Hit");
        hitBtn.addActionListener(new ActionListener() {
            @Override public void actionPerformed(ActionEvent e) {
                metrics.onHit();
            }
        });

        new Timer(100, new ActionListener() {
            @Override public void actionPerformed(ActionEvent evt) {
                rateLb.setText(Double.toString(metrics.getRate()));
            }
        }).start();

        setContentPane(createPanel(new JLabel("Hits in 5 seconds:"), rateLb, hitBtn));

        setMinimumSize(new Dimension(300, 120));
    }

    /**
     * @param components Components.
     * @return Panel.
     */
    private JPanel createPanel(JComponent... components) {
        JPanel panel = new JPanel();

        panel.setLayout(new FlowLayout());

        for (JComponent component : components)
            panel.add(component);

        return panel;
    }

    /**
     * @param args Args.
     */
    public static void main(String[] args) {
        EventQueue.invokeLater(new Runnable() {
            @Override public void run() {
                HitRateMetricsSandbox s = new HitRateMetricsSandbox();
                s.setVisible(true);
            }
        });
    }
}

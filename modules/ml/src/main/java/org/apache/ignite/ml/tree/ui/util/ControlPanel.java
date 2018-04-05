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

package org.apache.ignite.ml.tree.ui.util;

import java.awt.Color;
import java.awt.Dimension;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JSlider;

/**
 * Control panel used to change hyper parameters and clean the screen.
 */
public class ControlPanel extends JPanel {
    /** Default value of the "max deep" selector. */
    private static final int DEFAULT_MAX_DEEP = 2;

    /** Max value of the "max deep" selector. */
    private static final int MAX_MAX_DEEP = 20;

    /** Default value of the "min impurity decrease" selector. */
    private static final double DEFAULT_MIN_IMPURITY_DECREASE = 0;

    /** Max value of the "min impurity decrease" selector. */
    private static final double MAX_MIN_IMPURITY_DECREASE = 10;

    /** Change listeners. */
    private List<ControlPanelListener> listeners = new CopyOnWriteArrayList<>();

    /**
     * Constructs a new instance of control panel.
     */
    public ControlPanel() {
        setBackground(Color.decode("#313335"));
        setBorder(BorderFactory.createEtchedBorder());

        setPreferredSize(new Dimension(500, 120));

        add(createMaxDeepControlPanel());
        add(createMinImpurityDecreaseControlPanel());
        add(createCleanControlPanel());
    }

    /**
     * Constructs the "max deep" sub-panel.
     *
     * @return The "max deep" sub-panel.
     */
    private JPanel createMaxDeepControlPanel() {
        JPanel panel = new JPanel();
        panel.setBackground(Color.decode("#313335"));
        panel.setPreferredSize(new Dimension(500, 30));

        JLabel lb = new JLabel("Max deep: ");
        lb.setForeground(Color.decode("#869299"));
        panel.add(lb);

        JSlider slider = new JSlider();
        slider.setValue(DEFAULT_MAX_DEEP);
        slider.setMaximum(MAX_MAX_DEEP);
        slider.setBackground(Color.decode("#313335"));
        slider.setForeground(Color.decode("#869299"));
        panel.add(slider);

        slider.addChangeListener(e -> listeners.forEach(
            listener -> listener.doOnMaxDeepChange(slider.getValue())
        ));

        return panel;
    }

    /**
     * Constructs the "min impurity decrease" sub-panel.
     *
     * @return The "min impurity decrease" sub-panel.
     */
    private JPanel createMinImpurityDecreaseControlPanel() {
        JPanel panel = new JPanel();
        panel.setBackground(Color.decode("#313335"));
        panel.setPreferredSize(new Dimension(500, 30));

        JLabel lb = new JLabel("Min impurity decrease: ");
        lb.setForeground(Color.decode("#869299"));
        panel.add(lb);

        JSlider slider = new JSlider();
        slider.setValue((int) (DEFAULT_MIN_IMPURITY_DECREASE * 100));
        slider.setMaximum((int) (MAX_MIN_IMPURITY_DECREASE * 100));
        slider.setBackground(Color.decode("#313335"));
        slider.setForeground(Color.decode("#869299"));
        panel.add(slider);

        slider.addChangeListener(e -> listeners.forEach(
            listener -> listener.doOnMinImpurityDecreaseChange(slider.getValue() / 100.0)
        ));

        return panel;
    }

    /**
     * Constructs the "clean" sub-panel.
     *
     * @return The "clean" sub-panel.
     */
    private JPanel createCleanControlPanel() {
        JPanel panel = new JPanel();
        panel.setBackground(Color.decode("#313335"));
        panel.setPreferredSize(new Dimension(500, 30));

        JButton cleanBtn = new JButton("Clean");
        cleanBtn.setBackground(Color.decode("#313335"));
        cleanBtn.setRolloverEnabled(false);
        cleanBtn.setForeground(Color.decode("#869299"));
        panel.add(cleanBtn);

        cleanBtn.addActionListener(e -> listeners.forEach(ControlPanelListener::doOnClean));

        JButton generateBtn = new JButton("Generate");
        generateBtn.setBackground(Color.decode("#313335"));
        generateBtn.setForeground(Color.decode("#869299"));
        panel.add(generateBtn);

        generateBtn.addActionListener(e -> listeners.forEach(ControlPanelListener::doOnGenerate));

        return panel;
    }

    /** */
    public void addListener(ControlPanelListener lsnr) {
        listeners.add(lsnr);
    }

    /** */
    public void removeListener(ControlPanelListener lsnr) {
        listeners.remove(lsnr);
    }
}

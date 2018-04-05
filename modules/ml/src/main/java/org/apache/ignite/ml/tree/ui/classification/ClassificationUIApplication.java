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

package org.apache.ignite.ml.tree.ui.classification;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Point;
import java.awt.RenderingHints;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import javax.swing.BorderFactory;
import javax.swing.JPanel;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.tree.DecisionTreeConditionalNode;
import org.apache.ignite.ml.tree.DecisionTreeLeafNode;
import org.apache.ignite.ml.tree.ui.util.ControlPanelListener;

public abstract class ClassificationUIApplication extends JPanel implements ControlPanelListener {

    private final Map<Point, Boolean> points = new HashMap<>();

    private List<Rectangle> rectangles;

    private int maxDeep = 2;

    private double minImpurityDecrease = 0;

    public ClassificationUIApplication() {
        setBackground(Color.decode("#313335"));
        setBorder(BorderFactory.createEtchedBorder());

        setPreferredSize(new Dimension(500, 500));

        addMouseListener(new MouseAdapter() {
            @Override public void mousePressed(MouseEvent e) {
                switch (e.getButton()) {
                    case 1: {
                        points.put(new Point(e.getX(), e.getY()), true);
                        update();
                        break;
                    }
                    case 3: {
                        points.put(new Point(e.getX(), e.getY()), false);
                        update();
                        break;
                    }
                }
            }
        });
    }

    abstract Model<double[], Double> classify(double[][] x, double[] y, int maxDeep, double minImpurityDecrease);

    @Override
    protected void paintComponent(Graphics g) {
        super.paintComponent(g);

        Graphics2D graphics2D = (Graphics2D)g;
        graphics2D.setRenderingHint(
            RenderingHints.KEY_ANTIALIASING,
            RenderingHints.VALUE_ANTIALIAS_ON);

        if (rectangles != null) {
            for (Rectangle rectangle : rectangles) {
                g.setColor(Color.decode(rectangle.label == 1 ? "#7A3C3B" : "#465745"));
                g.fillRect(rectangle.x, rectangle.y, rectangle.width, rectangle.height);
            }
        }

        for (Map.Entry<Point, Boolean> e : points.entrySet()) {
            Point pnt = e.getKey();
            Boolean clazz = e.getValue();

            g.setColor(clazz ? Color.decode("#FF383C") : Color.decode("#35CD29"));
            g.fillOval(pnt.x - 2, pnt.y - 2, 4, 4);
        }
    }

    private void update() {
        double[][] x = new double[points.size()][];
        double[] y = new double[points.size()];

        if (!points.isEmpty()) {
            int ptr = 0;
            for (Map.Entry<Point, Boolean> e : points.entrySet()) {
                Point pnt = e.getKey();
                Boolean clazz = e.getValue();

                x[ptr] = new double[] {pnt.x, pnt.y};
                y[ptr] = clazz ? 1 : 0;

                ptr++;
            }

            Model<double[], Double> tree = classify(x, y, maxDeep, minImpurityDecrease);
            rectangles = toRectangles(tree, 0, 500, 0, 500);
        }
        else
            rectangles.clear();

        repaint();
    }

    private List<Rectangle> toRectangles(Model<double[], Double> node, int xFrom, int xTo, int yFrom, int yTo) {
        List<Rectangle> res = new ArrayList<>();

        if (node instanceof DecisionTreeLeafNode) {
            DecisionTreeLeafNode lf = (DecisionTreeLeafNode)node;
            res.add(new Rectangle(xFrom, yFrom, xTo - xFrom, yTo - yFrom, lf.getVal()));
        }
        else {
            DecisionTreeConditionalNode cn = (DecisionTreeConditionalNode)node;
            if (cn.getCol() == 0) {
                res.addAll(toRectangles(cn.getThenNode(), (int)cn.getThreshold(), xTo, yFrom, yTo));
                res.addAll(toRectangles(cn.getElseNode(), xFrom, (int)cn.getThreshold(), yFrom, yTo));
            }
            else {
                res.addAll(toRectangles(cn.getThenNode(), xFrom, xTo, (int)cn.getThreshold(), yTo));
                res.addAll(toRectangles(cn.getElseNode(), xFrom, xTo, yFrom, (int)cn.getThreshold()));
            }
        }

        return res;
    }

    @Override public void doOnMaxDeepChange(int maxDeep) {
        this.maxDeep = maxDeep;
        update();
    }

    @Override public void doOnMinImpurityDecreaseChange(double minImpurityDecrease) {
        this.minImpurityDecrease = minImpurityDecrease;
        update();
    }

    @Override public void doOnClean() {
        points.clear();
        update();
    }

    @Override public void doOnGenerate() {
        points.clear();

        Random rnd = new Random();
        for (int i = 0; i < 100; i++) {
            Point pnt = new Point(rnd.nextInt(500), rnd.nextInt(500));
            points.put(pnt, rnd.nextBoolean());
        }

        update();
    }

    private static class Rectangle {

        final int x;

        final int y;

        final int width;

        final int height;

        final double label;

        public Rectangle(int x, int y, int width, int height, double label) {
            this.x = x;
            this.y = y;
            this.width = width;
            this.height = height;
            this.label = label;
        }
    }
}

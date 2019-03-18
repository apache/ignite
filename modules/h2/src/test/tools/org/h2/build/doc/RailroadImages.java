/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.build.doc;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import javax.imageio.ImageIO;

/**
 * Create the images used in the railroad diagrams.
 */
public class RailroadImages {

    private static final int SIZE = 64;
    private static final int LINE_REPEAT = 32;
    private static final int DIV = 2;
    private static final int STROKE = 6;

    private String outDir;

    /**
     * This method is called when executing this application from the command
     * line.
     *
     * @param args the command line parameters
     */
    public static void main(String... args) {
        new RailroadImages().run("docs/html/images");
    }

    /**
     * Create the images.
     *
     * @param out the target directory
     */
    void run(String out) {
        this.outDir = out;
        new File(out).mkdirs();
        BufferedImage img;
        Graphics2D g;

        img = new BufferedImage(SIZE * 64, SIZE * LINE_REPEAT,
                BufferedImage.TYPE_INT_ARGB);
        g = img.createGraphics();
        for (int i = 0; i < 2; i++) {
            setStroke(g, i);
            g.drawLine(0, SIZE / 2, SIZE * 64, SIZE / 2);
        }
        g.dispose();
        savePng(img, "div-d.png");
        img = null;

        img = new BufferedImage(SIZE, SIZE * LINE_REPEAT, BufferedImage.TYPE_INT_ARGB);
        g = img.createGraphics();
        for (int i = 0; i < 2; i++) {
            setStroke(g, i);
            g.drawLine(0, SIZE / 2, SIZE, SIZE / 2);
            g.drawLine(SIZE / 2, SIZE, SIZE / 2, SIZE * LINE_REPEAT);
            // g.drawLine(0, SIZE / 2, SIZE / 2, SIZE);
            g.drawArc(-SIZE / 2, SIZE / 2, SIZE, SIZE, 0, 90);
        }
        g.dispose();
        savePng(img, "div-ts.png");
        savePng(flipHorizontal(img), "div-te.png");
        img = null;

        img = new BufferedImage(SIZE, SIZE * LINE_REPEAT, BufferedImage.TYPE_INT_ARGB);
        g = img.createGraphics();
        for (int i = 0; i < 2; i++) {
            setStroke(g, i);
            g.drawArc(SIZE / 2, -SIZE / 2, SIZE, SIZE, 180, 270);
            // g.drawLine(SIZE / 2, 0, SIZE, SIZE / 2);
        }
        savePng(img, "div-ls.png");
        savePng(flipHorizontal(img), "div-le.png");
        for (int i = 0; i < 2; i++) {
            setStroke(g, i);
            g.drawArc(SIZE / 2, -SIZE / 2, SIZE, SIZE, 180, 270);
            g.drawLine(SIZE / 2, 0, SIZE / 2, SIZE * LINE_REPEAT);
        }
        g.dispose();
        savePng(img, "div-ks.png");
        savePng(flipHorizontal(img), "div-ke.png");
        img = null;
    }

    private static void setStroke(Graphics2D g, int i) {
        if (i == 0) {
            g.setColor(Color.WHITE);
            g.setStroke(new BasicStroke(STROKE * 3));
        } else {
            g.setColor(Color.BLACK);
            g.setStroke(new BasicStroke(STROKE));
        }
    }

    private void savePng(BufferedImage img, String fileName) {
        int w = img.getWidth();
        int h = img.getHeight();
        BufferedImage smaller = new BufferedImage(w / DIV, h / DIV, img.getType());
        Graphics2D g = smaller.createGraphics();
        g.setRenderingHint(RenderingHints.KEY_INTERPOLATION,
                RenderingHints.VALUE_INTERPOLATION_BILINEAR);
        g.drawImage(img, 0, 0, w / DIV, h / DIV, 0, 0, w, h, null);
        g.dispose();
        try {
            ImageIO.write(smaller, "png", new File(outDir + "/" + fileName));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static BufferedImage flipHorizontal(BufferedImage img) {
        int w = img.getWidth(), h = img.getHeight();
        BufferedImage copy = new BufferedImage(w, h, img.getType());
        Graphics2D g = copy.createGraphics();
        g.drawImage(img, 0, 0, w, h, w, 0, 0, h, null);
        g.dispose();
        return copy;
    }

}

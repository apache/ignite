// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.hpc.executor;

import javax.imageio.*;
import java.awt.geom.*;
import java.awt.image.*;
import java.io.*;
import java.util.concurrent.*;

/**
 * Command contains simple logic for convert and scale images in JPEG format.
 * <p>
 * {@code GridExecutorImageScaleCommand} loads image from classpath with P2P class
 * loader from origin node and return result encapsulated in
 * {@link GridExecutorImage} object.
 * If {@code archive} variable equals {@code true} then all converted images
 * will be saved on nodes (involved in processing) in disk folder defined
 * by "java.io.tmpdir" system property.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridExecutorImageScaleCommand implements Callable<GridExecutorImage>, Serializable {
    /** */
    private static final String IMG_FORMAT = "jpg";

    /** */
    private double scale = 1;

    /** */
    private String rsrcName;

    /** */
    private boolean archive;

    /**
     * Creates command with necessary arguments for processing.
     *
     * @param scale Image scale parameter.
     * @param rsrcName Name of resource loaded by class loader.
     * @param archive Indicates if scaled images must be archived on disk folder.
     */
    public GridExecutorImageScaleCommand(double scale, String rsrcName, boolean archive) {
        assert rsrcName != null;

        this.scale = scale;
        this.rsrcName = rsrcName;
        this.archive = archive;
    }

    /**
     * Main command logic for image processing.
     *
     * @return Converted image.
     * @throws Exception If error occurred during processing.
     */
    @Override public GridExecutorImage call() throws Exception {
        BufferedImage srcImg = readImage();

        BufferedImage destImg = scale(scale, srcImg);

        archiveImage(destImg);

        System.out.println();
        System.out.println("Image scaled [height=" + destImg.getHeight() + ", width=" + destImg.getWidth() + ']');

        return prepareResult(destImg);
    }

    /**
     * Read image.
     *
     * @return Image object.
     * @throws IOException Thrown if image download failed.
     */
    private BufferedImage readImage() throws IOException {
        InputStream in = null;

        try {
            in = getClass().getClassLoader().getResourceAsStream(rsrcName);

            assert in != null : "Class loader can't find resource: " + rsrcName;

            in = new  BufferedInputStream(in);

            return ImageIO.read(in);
        }
        finally {
            close(in);
        }
    }

    /**
     * Scale image.
     *
     * @param scale Image scale parameter.
     * @param srcImg Source image for processing.
     * @return Converted image.
     */
    private BufferedImage scale(double scale, BufferedImage srcImg) {
        assert srcImg != null;

        if (Double.valueOf(scale).intValue() == 1) {
            return srcImg;
        }

        AffineTransform transform = AffineTransform.getScaleInstance(scale, scale);

        BufferedImageOp op = new AffineTransformOp(transform, null);

        return op.filter(srcImg, null);
    }

    /**
     * Archive image in folder defined by system property "java.io.tmpdir".
     *
     * @param img Image to archive.
     * @throws IOException Thrown if image saving failed.
     */
    private void archiveImage(RenderedImage img) throws IOException {
        if (!archive) {
            System.out.println("Image archive disabled.");

            return;
        }

        assert img != null;

        int idx = rsrcName.lastIndexOf('/');

        String fileName = idx == -1 ? rsrcName : rsrcName.substring(idx + 1);

        // Search for extension.
        idx = fileName.lastIndexOf('.');

        fileName = idx == -1 ? (fileName + '.' +  IMG_FORMAT) : (fileName.substring(0, idx + 1) + IMG_FORMAT);

        FileOutputStream out = null;

        try {
            out = new FileOutputStream(new File(System.getProperty("java.io.tmpdir"), fileName));

            ImageIO.write(img, IMG_FORMAT, out);
        }
        finally {
            close(out);
        }
    }

    /**
     * Convert to user known image object.
     *
     * @param img Image.
     * @return Converted image object.
     * @throws IOException Failed if image writing failed.
     */
    private GridExecutorImage prepareResult(RenderedImage img) throws IOException {
        ByteArrayOutputStream out = null;

        try {
            out = new ByteArrayOutputStream();

            ImageIO.write(img, IMG_FORMAT, out);

            return new GridExecutorImage(img.getHeight(), img.getWidth(), out.toByteArray());
        }
        finally {
            close(out);
        }
    }

    /**
     * Close resource.
     *
     * @param rsrc Resource object.
     */
    private void close(Closeable rsrc) {
        if (rsrc != null) {
            try {
                rsrc.close();
            }
            catch (IOException ignored) {
                // Ignore errors.
            }
        }
    }
}

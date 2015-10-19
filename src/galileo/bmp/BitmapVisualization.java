/*
Copyright (c) 2013, Colorado State University
All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
   list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.

This software is provided by the copyright holders and contributors "as is" and
any express or implied warranties, including, but not limited to, the implied
warranties of merchantability and fitness for a particular purpose are
disclaimed. In no event shall the copyright holder or contributors be liable for
any direct, indirect, incidental, special, exemplary, or consequential damages
(including, but not limited to, procurement of substitute goods or services;
loss of use, data, or profits; or business interruption) however caused and on
any theory of liability, whether in contract, strict liability, or tort
(including negligence or otherwise) arising in any way out of the use of this
software, even if advised of the possibility of such damage.
*/

package galileo.bmp;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.File;

import java.io.IOException;
import java.util.Iterator;

import javax.imageio.ImageIO;

/**
 * Provides functionality for visualizing bitmaps.
 *
 * @author malensek
 */
public class BitmapVisualization {

    /**
     * Draws a {@link Bitmap} instance as a BufferedImage.
     *
     * @param bitmap The bitmap to draw.
     * @param width Number of bits to draw before wrapping to the next line
     * @param height Number of scanlines in the bitmap
     */
    public static BufferedImage drawBitmap(
            Bitmap bitmap, int width, int height) {

        return drawIterableMap(bitmap.iterator(), width, height);
    }

    /**
     * Draws a {@link Bitmap} instance as a BufferedImage with a particular
     * color used to indicate the areas of the bitmap set to 1.
     *
     * @param bitmap The bitmap to draw.
     * @param width Number of bits to draw before wrapping to the next line
     * @param height Number of scanlines in the bitmap
     * @param color {@link java.awt.Color} to use for bits that are set to 1.
     */
    public static BufferedImage drawBitmap(
            Bitmap bitmap, int width, int height, Color color) {

        return drawIterableMap(bitmap.iterator(), width, height, color);
    }

    /**
     * Convenience function to draw a {@link GeoavailabilityGrid} as a
     * {@link BufferedImage}.
     *
     * @param g GeoavailabilityGrid to draw
     */
    public static BufferedImage drawGeoavailabilityGrid(GeoavailabilityGrid g) {
        return drawIterableMap(g.getBitmap().iterator(),
                g.getWidth(), g.getHeight());
    }

    /**
     * Convenience function to draw a {@link GeoavailabilityGrid} as a
     * {@link BufferedImage}, with a specific color for the bits that are set
     * to 1.
     *
     * @param g GeoavailabilityGrid to draw
     * @param color {@link java.awt.Color} to use for bits that are set to 1.
     */
    public static BufferedImage drawGeoavailabilityGrid(
            GeoavailabilityGrid g, Color color) {
        return drawIterableMap(g.getBitmap().iterator(),
                g.getWidth(), g.getHeight(), color);
    }

    /**
     * Given an iterator of integer indices in a bit set, this method will draw
     * the visual representation of the set.  This is useful for visualizing
     * what a bitmap index or query polygon looks like.
     *
     * @param it Iterator of bit set indices.
     * @param width Width of the output image.
     * @param height Height of the output image.
     *
     * @return A BufferedImage with the visual representation of the bitset.
     */
    public static BufferedImage drawIterableMap(Iterator<Integer> it,
            int width, int height) {

        return drawIterableMap(it, width, height, Color.WHITE);
    }

    /**
     * Given an iterator of integer indices in a bit set, this method will draw
     * the visual representation of the set.  This is useful for visualizing
     * what a bitmap index or query polygon looks like.
     *
     * @param it Iterator of bit set indices.
     * @param width Width of the output image.
     * @param height Height of the output image.
     * @param color {@link java.awt.Color} of bits set to 1.
     *
     * @return A BufferedImage with the visual representation of the bitset.
     */
    public static BufferedImage drawIterableMap(Iterator<Integer> it,
            int width, int height, Color color) {

        BufferedImage img = new BufferedImage(width, height,
                BufferedImage.TYPE_INT_ARGB);

        while (it.hasNext()) {
            int index = it.next();
            int x = index % width;
            int y = index / width;
            if(x >= 0 && x < width && y >= 0 && y < height)
            	img.setRGB(x, y, color.getRGB());
        }

        return img;
    }

    /**
     * Convenience function to write a BufferedImage to a file.
     */
    public static void imageToFile(BufferedImage image, String file)
    throws IOException {
        File f = new File(file);
        ImageIO.write(image, "gif", f);
    }
}

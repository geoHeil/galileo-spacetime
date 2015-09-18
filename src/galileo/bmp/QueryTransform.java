/*
Copyright (c) 2014, Colorado State University
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

import galileo.dataset.Coordinates;

import galileo.dataset.Point;

import java.awt.Graphics2D;
import java.awt.Polygon;
import java.awt.Rectangle;
import java.awt.geom.AffineTransform;
import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Provides functionality for transforming {@link GeoavailabilityQuery}
 * instances into Bitmaps that can be used with a {@link GeoavailabilityGrid}.
 *
 * @author malensek
 */
public class QueryTransform {

    protected static final Logger logger = Logger.getLogger("galileo");

    public static Bitmap queryToGridBitmap(
            GeoavailabilityQuery query, GeoavailabilityGrid grid) {

        /* Convert lat, lon coordinates into x, y points on the grid */
        List<Coordinates> poly = query.getPolygon();
        Polygon p = new Polygon();
        for (Coordinates coords : poly) {
            Point<Integer> point = grid.coordinatesToXY(coords);
            p.addPoint(point.X(), point.Y());
        }

        /* Determine the minimum bounding rectangle (MBR) of the polygon. */
        Rectangle boundingBox = p.getBounds();
        int x = (int) boundingBox.getX();
        int y = (int) boundingBox.getY();
        int w = (int) boundingBox.getWidth();
        int h = (int) boundingBox.getHeight();

        /* Calculate shift factors.  This method outputs Bitmaps that are
         * word-aligned (64 bit boundaries).  If the extra width or height added
         * overflows, then the shift factors are adjusted accordingly. */
        int xshift = 0;
        int yshift = 0;
        int wshift = 64 - (w % 64);
        int hshift = 64 - (h % 64);

        if (w + wshift >= grid.getWidth()) {
            int overflow = grid.getWidth() - (w + wshift) + 1;
            wshift = wshift - overflow;
            xshift = overflow;
        }

        if (h + hshift >= grid.getHeight()) {
            int overflow = grid.getHeight() - (h + hshift) + 1;
            hshift = hshift - overflow;
            yshift = overflow;
        }

        w = w + wshift;
        h = h + hshift;

        if (logger.isLoggable(Level.INFO)) {
            logger.log(Level.INFO, "Converting query polygon to "
                    + "GeoavailabilityGrid bitmap. {0}x{1} at ({2}, {3});"
                    + " shifts: +({4}, {5}) -({6}, {7})",
                    new Object[] {
                        w, h, x, y, wshift, hshift, xshift, yshift
                    });
        }

        BufferedImage img = new BufferedImage(w, h,
                BufferedImage.TYPE_BYTE_INDEXED);
        Graphics2D g = img.createGraphics();

        /* Apply these x, y transformations to the resulting image */
        AffineTransform transform = new AffineTransform();
        transform.translate(-x, -y);
        transform.translate(-xshift, -yshift);
        g.setTransform(transform);

        g.fillPolygon(p);
        g.dispose();

        /* Get the raw image data, in bytes */
        DataBufferByte buffer =
            ((DataBufferByte) img.getData().getDataBuffer());
        byte[] data = buffer.getData();

        /* Convert the bytes into a Bitmap representation */
        Bitmap queryBitmap = Bitmap.fromBytes(data, x, y, w, h,
                grid.getWidth(), grid.getHeight());

        return queryBitmap;
    }
}

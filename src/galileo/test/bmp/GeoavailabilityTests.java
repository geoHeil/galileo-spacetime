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

package galileo.test.bmp;

import static org.junit.Assert.*;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.IOException;

import java.util.ArrayList;
import java.util.List;

import galileo.bmp.GeoavailabilityGrid;
import galileo.bmp.GeoavailabilityQuery;
import galileo.bmp.QueryTransform;
import galileo.bmp.BitmapVisualization;
import galileo.dataset.Coordinates;
import galileo.dataset.SpatialRange;

import org.junit.Test;

public class GeoavailabilityTests {

    private boolean draw = false;

    public GeoavailabilityTests() {
        this.draw = Boolean.parseBoolean(System.getProperty(
                "galileo.test.bmp.GeoavailabilityTests.draw",
                "false"));
    }

    /**
     * Test converting X, Y grid coordinates to spatial ranges using the
     * calculated corners of the grid.
     */
    @Test
    public void testXYtoSpatialRange() {
        GeoavailabilityGrid gg = new GeoavailabilityGrid("9x", 10);
        gg.addPoint(new Coordinates(44.919f, -112.242f));
        gg.addPoint(new Coordinates(44.919f, -101.514f));
        gg.addPoint(new Coordinates(39.496f, -112.242f));
        gg.addPoint(new Coordinates(39.496f, -101.514f));


        float epsilon = 0.01f;

        SpatialRange s1 = gg.XYtoSpatialRange(0, 0);
        SpatialRange s2 = gg.XYtoSpatialRange(31, 0);
        SpatialRange s3 = gg.XYtoSpatialRange(0, 31);
        SpatialRange s4 = gg.XYtoSpatialRange(31, 31);

        assertEquals(s1.getLowerBoundForLatitude(), 45.0f, epsilon);
        assertEquals(s1.getLowerBoundForLongitude(), -112.5f, epsilon);
        assertEquals(s1.getUpperBoundForLatitude(), 44.82f, epsilon);
        assertEquals(s1.getUpperBoundForLongitude(), -112.14f, epsilon);
        assertEquals(s2.getLowerBoundForLatitude(), 45.0f, epsilon);
        assertEquals(s2.getLowerBoundForLongitude(), -101.60f, epsilon);
        assertEquals(s2.getUpperBoundForLatitude(), 44.82f, epsilon);
        assertEquals(s2.getUpperBoundForLongitude(), -101.25f, epsilon);
        assertEquals(s3.getLowerBoundForLatitude(), 39.55f, epsilon);
        assertEquals(s3.getLowerBoundForLongitude(), -112.5f, epsilon);
        assertEquals(s3.getUpperBoundForLatitude(), 39.37f, epsilon);
        assertEquals(s3.getUpperBoundForLongitude(), -112.14f, epsilon);
        assertEquals(s4.getLowerBoundForLatitude(), 39.55f, epsilon);
        assertEquals(s4.getLowerBoundForLongitude(), -101.60f, epsilon);
        assertEquals(s4.getUpperBoundForLatitude(), 39.37f, epsilon);
        assertEquals(s4.getUpperBoundForLongitude(), -101.25f, epsilon);
    }

    @Test
    public void testQuery() throws Exception {
        GeoavailabilityGrid gg = new GeoavailabilityGrid("9x", 20);
        gg.addPoint(new Coordinates(43.438f, -110.300f));
        List<Coordinates> p1 = new ArrayList<>();
        p1.add(new Coordinates(44.919f, -112.242f));
        p1.add(new Coordinates(43.111f, -105.414f));
        p1.add(new Coordinates(41.271f, -111.421f));
        GeoavailabilityQuery q1 = new GeoavailabilityQuery(p1);
        assertEquals(true, gg.intersects(q1));
        if (draw) {
            BufferedImage b = BitmapVisualization.drawBitmap(
                    QueryTransform.queryToGridBitmap(q1, gg),
                    gg.getWidth(), gg.getHeight(), Color.BLACK);
            BitmapVisualization.imageToFile(b, "Query1.gif");
        }

        List<Coordinates> p2 = new ArrayList<>();
        p2.add(new Coordinates(41.223f, -101.609f));
        p2.add(new Coordinates(39.663f, -101.641f));
        p2.add(new Coordinates(39.745f, -104.701f));
        GeoavailabilityQuery q2 = new GeoavailabilityQuery(p2);
        assertEquals(false, gg.intersects(q2));
        if (draw) {
            BufferedImage b = BitmapVisualization.drawBitmap(
                    QueryTransform.queryToGridBitmap(q2, gg),
                    gg.getWidth(), gg.getHeight(), Color.BLACK);
            BitmapVisualization.imageToFile(b, "Query2.gif");
        }
    }

    @Test
    public void testBitmapCorners() throws IOException {
        GeoavailabilityGrid gg = new GeoavailabilityGrid("9x", 10);
        gg.addPoint(new Coordinates(44.919f, -112.242f));
        gg.addPoint(new Coordinates(44.919f, -101.514f));
        gg.addPoint(new Coordinates(39.496f, -112.242f));
        gg.addPoint(new Coordinates(39.496f, -101.514f));

        if (draw) {
            BufferedImage b = BitmapVisualization.drawGeoavailabilityGrid(
                    gg, Color.BLACK);
            BitmapVisualization.imageToFile(b, "BitmapCorners.gif");
        }
    }

    @Test
    public void testHiResCorners() throws IOException {
        GeoavailabilityGrid gg = new GeoavailabilityGrid("9x", 25);
        gg.addPoint(new Coordinates(44.919f, -112.242f));
        gg.addPoint(new Coordinates(44.919f, -101.514f));
        gg.addPoint(new Coordinates(39.496f, -112.242f));
        gg.addPoint(new Coordinates(39.496f, -101.514f));

        if (draw) {
            BufferedImage b = BitmapVisualization.drawGeoavailabilityGrid(
                    gg, Color.BLACK);
            BitmapVisualization.imageToFile(b, "HiResCorners.gif");
        }
    }

    /**
     * Ensures out-of-bounds X points are not inserted into the
     * GeoavailabilityGrid.
     */
    @Test
    public void testXOutOfBounds() {
        GeoavailabilityGrid gg = new GeoavailabilityGrid("9x", 10);
        assertEquals(gg.addPoint(new Coordinates(44.819f, -113.350f)), false);
        assertEquals(gg.addPoint(new Coordinates(44.819f, -100.684f)), false);
        assertEquals(gg.addPoint(new Coordinates(39.496f, -113.350f)), false);
        assertEquals(gg.addPoint(new Coordinates(39.496f, -100.684f)), false);
        assertEquals(gg.addPoint(new Coordinates(0.0f, 0.0f)), false);
    }

    /**
     * Ensures out-of-bounds Y points are not inserted into the
     * GeoavailabilityGrid.
     */
    @Test
    public void testYOutOfBounds() {
        GeoavailabilityGrid gg = new GeoavailabilityGrid("9x", 10);
        assertEquals(gg.addPoint(new Coordinates(45.333f, -112.242f)), false);
        assertEquals(gg.addPoint(new Coordinates(45.360f, -101.514f)), false);
        assertEquals(gg.addPoint(new Coordinates(38.992f, -112.242f)), false);
        assertEquals(gg.addPoint(new Coordinates(38.992f, -101.514f)), false);
        assertEquals(gg.addPoint(new Coordinates(0.0f, 0.0f)), false);
    }

    /**
     * Tests the update procedure for bits that are inserted out of order.
     */
    @Test
    public void testUpdates() throws Exception {
        /* Insert points in indexed order */
        GeoavailabilityGrid g1 = new GeoavailabilityGrid("9x", 10);
        g1.addPoint(new Coordinates(44.919f, -112.242f));
        g1.addPoint(new Coordinates(44.919f, -101.514f));
        g1.addPoint(new Coordinates(39.496f, -112.242f));
        g1.addPoint(new Coordinates(39.496f, -101.514f));

        /* Insert points out of order */
        GeoavailabilityGrid g2 = new GeoavailabilityGrid("9x", 10);
        g2.addPoint(new Coordinates(39.496f, -101.514f));
        g2.addPoint(new Coordinates(39.496f, -112.242f));
        g2.addPoint(new Coordinates(44.919f, -101.514f));
        g2.addPoint(new Coordinates(44.919f, -112.242f));

        if (draw) {
            BufferedImage b1 = BitmapVisualization.drawGeoavailabilityGrid(g1);
            BufferedImage b2 = BitmapVisualization.drawGeoavailabilityGrid(g2);
            BitmapVisualization.imageToFile(b1, "Updates1.gif");
            BitmapVisualization.imageToFile(b2, "Updates2.gif");
        }
        assertEquals(true, g1.equals(g2));
    }
}

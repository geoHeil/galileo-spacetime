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

import java.util.SortedSet;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import galileo.dataset.Coordinates;
import galileo.dataset.Point;
import galileo.dataset.SpatialRange;
import galileo.util.GeoHash;

/**
 * Implements a bitmap-based spatial index that can be used to determine
 * whether or not data is stored within specified spatial regions.
 *
 * @author malensek
 */
public class GeoavailabilityGrid {

    private static final Logger logger = Logger.getLogger("galileo");

    private int width, height;

    public Bitmap bmp = new Bitmap();
    private SortedSet<Integer> pendingUpdates = new TreeSet<>();

    private SpatialRange baseRange;
    private float xDegreesPerPixel;
    private float yDegreesPerPixel;

    public GeoavailabilityGrid(String baseGeohash, int precision) {
        this.baseRange = GeoHash.decodeHash(baseGeohash);

        /*
         * height, width calculated like so:
         * width = 2^(floor(precision / 2))
         * height = 2^(ceil(precision / 2))
         */
        int w = precision / 2;
        int h = precision / 2;
        if (precision % 2 != 0) {
            h += 1;
        }

        this.width = (1 << w); /* = 2^w */
        this.height = (1 << h); /* = 2^h */

        /* Determine the number of degrees in the x and y directions for the
         * base spatial range this geoavailability grid represents */
        float xDegrees = baseRange.getUpperBoundForLongitude()
            - baseRange.getLowerBoundForLongitude();
        float yDegrees = baseRange.getUpperBoundForLatitude()
            - baseRange.getLowerBoundForLatitude();

        /* Determine the number of degrees represented by each grid pixel */
        xDegreesPerPixel = xDegrees / (float) this.width;
        yDegreesPerPixel = yDegrees / (float) this.width;

        if (logger.isLoggable(Level.INFO)) {
            logger.log(Level.INFO, "Created geoavailability grid: "
                    + "geohash={0}, precision={1}, "
                    + "width={2}, height={3}, baseRange={6}, "
                    + "xDegreesPerPixel={4}, yDegreesPerPixel={5}",
                    new Object[] { baseGeohash, precision, width, height,
                        xDegreesPerPixel, yDegreesPerPixel, baseRange});
        }
    }

    /**
     * Adds a new point to this GeoavailabilityGrid.
     *
     * @param coords The location (coordinates in lat, lon) to add.
     *
     * @return true if the point could be added to grid, false otherwise (for
     * example, if the point falls outside the purview of the grid)
     */
    public boolean addPoint(Coordinates coords) {
        Point<Integer> gridPoint = coordinatesToXY(coords);
        int index = XYtoIndex(gridPoint.X(), gridPoint.Y());

        if (gridPoint.X() < 0 || gridPoint.X() >= this.width
                || gridPoint.Y() < 0 || gridPoint.Y() >= this.height) {

            return false;
        }

        if (this.bmp.set(index) == false) {
            /* Could not set the bits now; add to pending updates */
            pendingUpdates.add(index);
        }
        return true;
    }

    /**
     * Converts a coordinate pair (defined with latitude, longitude in decimal
     * degrees) to an x, y location in the grid.
     *
     * @param coords the Coordinates to convert.
     *
     * @return Corresponding x, y location in the grid.
     */
    public Point<Integer> coordinatesToXY(Coordinates coords) {

        /* Assuming (x, y) coordinates for the geoavailability grids, latitude
         * will decrease as y increases, and longitude will increase as x
         * increases. This is reflected in how we compute the differences
         * between the base points and the coordinates in question. */
        float xDiff = coords.getLongitude()
            - baseRange.getLowerBoundForLongitude();

        float yDiff = baseRange.getUpperBoundForLatitude()
            - coords.getLatitude();

        int x = (int) (xDiff / xDegreesPerPixel);
        int y = (int) (yDiff / yDegreesPerPixel);

        return new Point<>(x, y);
    }

    /**
     * Converts X, Y coordinates to a particular index within the underlying
     * bitmap implementation.  Essentially this converts a 2D index to a 1D
     * index.
     *
     * @param x The x coordinate to convert
     * @param y The y coorddinate to convert
     *
     * @return A single integer representing the bitmap location of the X, Y
     * coordinates.
     */
    public int XYtoIndex(int x, int y) {
        return y * this.width + x;
    }

    /**
     * Converts a bitmap index to X, Y coordinates in the grid.
     */
    public Point<Integer> indexToXY(int index) {
        int x = index % this.width;
        int y = index / this.width;
        return new Point<>(x, y);
    }

    /**
     * Converts an X, Y grid point to the corresponding SpatialRange that the
     * grid point spans.
     * (0, 0) indicates (90, -180)
     */
    public SpatialRange XYtoSpatialRange(int x, int y) {
        if (x < 0 || x >= this.width || y < 0 || y >= this.height) {
            throw new IllegalArgumentException(
                    "Out-of-bounds grid coordinates specified");
        }

        float baseLon = baseRange.getLowerBoundForLongitude();
        float baseLat = baseRange.getUpperBoundForLatitude();

        float lowerLon = baseLon + (x * xDegreesPerPixel);
        float upperLon = lowerLon + xDegreesPerPixel;
        float upperLat = baseLat - (y * yDegreesPerPixel);
        float lowerLat = upperLat - yDegreesPerPixel;

        return new SpatialRange(lowerLat, upperLat, lowerLon, upperLon);
    }

    /**
     * Converts a bitmap index location to a corresponding SpatialRange that
     * the indexed grid point spans.
     */
    public SpatialRange indexToSpatialRange(int index) {
        Point<Integer> gridLocation = indexToXY(index);
        return XYtoSpatialRange(gridLocation.X(), gridLocation.Y());
    }

    /**
     * Applies pending updates that have not yet been integrated into the
     * GeoavailabilityGrid instance.
     */
    private void applyUpdates() {
        Bitmap updateBitmap = new Bitmap();
        for (int i : pendingUpdates) {
            if (updateBitmap.set(i) == false) {
                logger.warning("Could not set update bit");
            }
        }
        pendingUpdates.clear();

        this.bmp = this.bmp.or(updateBitmap);
    }

    /**
     * Reports whether or not the supplied {@link GeoavailabilityQuery}
     * instance intersects with the bits set in this geoavailability grid.  This
     * operation can be much faster than performing a full inspection of what
     * bits are actually set.
     *
     * @param query The query geometry to test for intersection.
     *
     * @return true if the supplied {@link GeoavailabilityQuery} intersects with
     * the data in the geoavailability grid.
     */
    public boolean intersects(GeoavailabilityQuery query)
    throws BitmapException {
        applyUpdates();
        Bitmap queryBitmap = QueryTransform.queryToGridBitmap(query, this);
        return this.bmp.intersects(queryBitmap);
    }

    /**
     * Queries the geoavailability grid, which involves performing a logical AND
     * operation and reporting the resulting Bitmap.
     *
     * @param query The query geometry to evaluate against the geoavailability
     * grid.
     *
     * @return An array of bitmap indices that matched the query.
     */
    public int[] query(GeoavailabilityQuery query)
    throws BitmapException {
        applyUpdates();
        Bitmap queryBitmap = QueryTransform.queryToGridBitmap(query, this);
        if(this.bmp.intersects(queryBitmap))
        	return this.bmp.and(queryBitmap).toArray();
    	return null;
    }

    /**
     * Retrieves the underlying Bitmap instance backing this
     * GeoavailabilityGrid.
     */
    protected Bitmap getBitmap() {
        applyUpdates();
        return bmp;
    }

    /**
     * Retrieves the width of this GeoavailabilityGrid, in grid cells.
     */
    public int getWidth() {
        return width;
    }

    /**
     * Retrieves the height of this GeoavailabilityGrid, in grid cells.
     */
    public int getHeight() {
        return height;
    }

    /**
     * Retrieves the base SpatialRange that this GeoavailabilityGrid is
     * responsible for; the base range defines the geographic scope of this
     * GeoavailabilityGrid instance.
     *
     * @return {@link SpatialRange} representing this GeoavailabilityGrid's
     * scope.
     */
    public SpatialRange getBaseRange() {
        return new SpatialRange(baseRange);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
            + ((baseRange == null) ? 0 : baseRange.hashCode());
        result = prime * result + ((bmp == null) ? 0 : bmp.hashCode());
        result = prime * result + height;
        result = prime * result + width;
        result = prime * result + Float.floatToIntBits(xDegreesPerPixel);
        result = prime * result + Float.floatToIntBits(yDegreesPerPixel);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        GeoavailabilityGrid other = (GeoavailabilityGrid) obj;
        if (baseRange == null) {
            if (other.baseRange != null) {
                return false;
            }
        } else if (!baseRange.equals(other.baseRange)) {
            return false;
        }
        if (bmp == null) {
            if (other.bmp != null) {
                return false;
            }
        } else if (!bmp.equals(other.bmp)) {
            return false;
        }
        if (height != other.height) {
            return false;
        }
        if (width != other.width) {
            return false;
        }
        if (Float.floatToIntBits(xDegreesPerPixel) != Float
                .floatToIntBits(other.xDegreesPerPixel)) {
            return false;
                }
        if (Float.floatToIntBits(yDegreesPerPixel) != Float
                .floatToIntBits(other.yDegreesPerPixel)) {
            return false;
                }
        return true;
    }
}

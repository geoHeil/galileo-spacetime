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
import galileo.dataset.SpatialRange;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Similar to the GeoavailabilityGrid, the GeoavailabilityMap allows arbitrary
 * spatial locations to be mapped to lists of data points.
 *
 * @author malensek
 */
public class GeoavailabilityMap<T> {

    private GeoavailabilityGrid grid;
    private Map<Integer, List<T>> points;

    public GeoavailabilityMap(String baseGeohash, int precision) {
        grid = new GeoavailabilityGrid(baseGeohash, precision);
        points = new HashMap<>();
    }

    /**
     * Adds a new point to this GeoavailabilityMap, and associates it with a
     * data point.
     *
     * @param coords The location (coordinates in lat, lon) to add.
     * @param data Data point to associate with the location.
     *
     * @return true if the point could be added to map, false otherwise (for
     * example, if the point falls outside the purview of the grid)
     */
    public boolean addPoint(Coordinates coords, T data) {
        if (grid.addPoint(coords) == false) {
            return false;
        }

        Point<Integer> gridPoint = grid.coordinatesToXY(coords);
        int index = grid.XYtoIndex(gridPoint.X(), gridPoint.Y());

        List<T> dataList = points.get(index);
        if (dataList == null) {
            dataList = new ArrayList<>();
            points.put(index, dataList);
        }

        dataList.add(data);
        return true;
    }

    /**
     * Retrieves the {@link SpatialRange} represented by a grid index point.
     */
    public SpatialRange indexToSpatialRange(int index) {
        return grid.indexToSpatialRange(index);
    }

    /**
     * Queries the GeoavailabilityGrid, returning the grid indices that contain
     * matching points, with lists of their associated data points.
     *
     * @param query GeoavailabilityQuery to evaluate
     *
     * @return results, as a Map of indices to arrays of data points.
     */
    public Map<Integer, List<T>> query(GeoavailabilityQuery query)
    throws BitmapException {
        Map<Integer, List<T>> results = new HashMap<>();
        int[] indices = grid.query(query);
        
        if(null != indices){
		    for (int i : indices) {
		        results.put(i, points.get(i));
		    }
        }

        return results;
    }
    
    public GeoavailabilityGrid getGrid(){
    	return this.grid;
    }
}

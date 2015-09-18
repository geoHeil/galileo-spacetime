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

package galileo.dht.hash;

import galileo.dataset.SpatialProperties;
import galileo.util.GeoHash;

import java.math.BigInteger;
import java.util.Random;

/**
 * Provides a Geohash-based hash function for spatial data.
 *
 * @author malensek
 */
public class SpatialHash implements HashFunction<SpatialProperties> {

    private static final int PRECISION = 12;

    private Random random = new Random();

    @Override
    public BigInteger hash(SpatialProperties spatialProps)
    throws HashException {

        String hash = "";

        if (spatialProps.hasRange()) {
            hash = GeoHash.encode(spatialProps.getSpatialRange(), PRECISION);
        } else {
            hash = GeoHash.encode(spatialProps.getCoordinates(), PRECISION);
        }

        return BigInteger.valueOf(GeoHash.hashToLong(hash));
    }

    @Override
    public BigInteger maxValue() {
        /* 12 chars * 5 bits/char = 60 bits for a 12-char hash. */
        return BigInteger.valueOf(2).pow(PRECISION * GeoHash.BITS_PER_CHAR);
    }

    @Override
    public BigInteger randomHash() {
        float lat = random.nextFloat() * GeoHash.LATITUDE_RANGE
            * (random.nextBoolean() ? 1 : -1 ); // randomly negate

        float lon = random.nextFloat() * GeoHash.LONGITUDE_RANGE
            * (random.nextBoolean() ? 1 : -1 );

        String hash = GeoHash.encode(lat, lon, PRECISION);
        return BigInteger.valueOf(GeoHash.hashToLong(hash));
    }
}

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

import galileo.dataset.Metadata;
import galileo.dataset.SpatialProperties;
import galileo.util.GeoHash;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

/**
 * Provides a Geohash-based hash function that acts on a predefined constrained
 * set of hashes.
 *
 * @author malensek
 */
public class ConstrainedGeohash implements HashFunction<Metadata> {

	private Random random = new Random();

	private String[] geohashes;
	private Map<String, BigInteger> hashMappings = new HashMap<>();
	private int precision;

	public ConstrainedGeohash(String[] geohashes) throws HashException {

		this.geohashes = geohashes;

		/* All Geohashes are assumed to be a consistent precision . */
		precision = geohashes[0].length();

		for (String hash : geohashes) {
			/* Check for proper precision */
			if (hash.length() != precision) {
				throw new HashException("ConstrainedGeohash requires "
						+ "consistent Geohash precision");
			}

			int idx = hashMappings.keySet().size();
			hashMappings.put(hash.toLowerCase(), BigInteger.valueOf(idx));
		}
	}

	@Override
	public BigInteger hash(Metadata data) throws HashException {
		String hash = null;
		SpatialProperties spatialProps = data.getSpatialProperties();

		if (spatialProps.hasRange()) {
			hash = GeoHash.encode(spatialProps.getSpatialRange(), precision);
		} else {
			hash = GeoHash.encode(spatialProps.getCoordinates(), precision);
		}

		BigInteger position = hashMappings.get(hash);
		if (position == null) {
			throw new HashException("Could not find position in hash space.");
		}

		return position;
	}

	/**
	 * @param value:
	 *            The value obtained by hashing the metadata. In other words,
	 *            the value returned by the hash(Metadata) method call
	 * @return The Geo-Hash value corresponding to the position of the metadata.
	 */
	public String getGeoHash(BigInteger value) {
		Set<Entry<String, BigInteger>> entrySet = this.hashMappings.entrySet();
		for(Entry<String, BigInteger> entry : entrySet){
			if(entry.getValue().equals(value))
				return entry.getKey();
		}
		return null;
	}

	@Override
	public BigInteger maxValue() {
		return BigInteger.valueOf(geohashes.length);
	}

	@Override
	public BigInteger randomHash() {
		int idx = random.nextInt(geohashes.length);
		return hashMappings.get(geohashes[idx]);
	}
}

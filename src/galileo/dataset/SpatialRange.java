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

package galileo.dataset;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import galileo.serialization.ByteSerializable;
import galileo.serialization.SerializationException;
import galileo.serialization.SerializationInputStream;
import galileo.serialization.SerializationOutputStream;
import galileo.util.Pair;

public class SpatialRange implements ByteSerializable {
	private float upperLat;
	private float lowerLat;
	private float upperLon;
	private float lowerLon;

	private boolean hasElevation;
	private float upperElevation;
	private float lowerElevation;

	private List<Coordinates> polygon;

	public SpatialRange(List<Coordinates> polygon) {
		for (Coordinates coords : polygon) {
			if (this.polygon == null) {
				this.lowerLat = this.upperLat = coords.getLatitude();
				this.lowerLon = this.upperLon = coords.getLongitude();
				this.polygon = polygon;
			} else {
				if (coords.getLatitude() < this.lowerLat)
					this.lowerLat = coords.getLatitude();

				if (coords.getLatitude() > this.upperLat)
					this.upperLat = coords.getLatitude();

				if (coords.getLongitude() < this.lowerLon)
					this.lowerLon = coords.getLongitude();

				if (coords.getLongitude() > this.upperLon)
					this.upperLon = coords.getLongitude();
			}
		}
	}

	public SpatialRange(float lowerLat, float upperLat, float lowerLon, float upperLon) {
		this.lowerLat = lowerLat;
		this.upperLat = upperLat;
		this.lowerLon = lowerLon;
		this.upperLon = upperLon;

		hasElevation = false;
	}

	public SpatialRange(float lowerLat, float upperLat, float lowerLon, float upperLon, float upperElevation,
			float lowerElevation) {
		this.lowerLat = lowerLat;
		this.upperLat = upperLat;
		this.lowerLon = lowerLon;
		this.upperLon = upperLon;

		hasElevation = true;
		this.upperElevation = upperElevation;
		this.lowerElevation = lowerElevation;
	}

	public SpatialRange(SpatialRange copyFrom) {
		this.lowerLat = copyFrom.lowerLat;
		this.upperLat = copyFrom.upperLat;
		this.lowerLon = copyFrom.lowerLon;
		this.upperLon = copyFrom.upperLon;

		this.hasElevation = copyFrom.hasElevation;
		this.upperElevation = copyFrom.upperElevation;
		this.lowerElevation = copyFrom.lowerElevation;
	}

	/*
	 * Retrieves the smallest latitude value of this spatial range
	 */
	public float getLowerBoundForLatitude() {
		return lowerLat;
	}

	/*
	 * Retrieves the largest latitude value of this spatial range
	 */
	public float getUpperBoundForLatitude() {
		return upperLat;
	}

	/*
	 * Retrieves the smallest longitude value of this spatial range
	 */
	public float getLowerBoundForLongitude() {
		return lowerLon;
	}

	/*
	 * Retrieves the largest longitude value of this spatial range
	 */
	public float getUpperBoundForLongitude() {
		return upperLon;
	}

	public Coordinates getCenterPoint() {
		float latDifference = upperLat - lowerLat;
		float latDistance = latDifference / 2;

		float lonDifference = upperLon - lowerLon;
		float lonDistance = lonDifference / 2;

		return new Coordinates(lowerLat + latDistance, lowerLon + lonDistance);
	}
	
	public List<Coordinates> getBounds(){
		List<Coordinates> box = new ArrayList<Coordinates>();
		box.add(new Coordinates(upperLat, lowerLon));
		box.add(new Coordinates(upperLat, upperLon));
		box.add(new Coordinates(lowerLat, upperLon));
		box.add(new Coordinates(lowerLat, lowerLon));
		return box;
	}

	/**
	 * Using the upper and lower boundaries for this spatial range, generate two
	 * lat, lon points that represent the upper-left and lower-right coordinates
	 * of the range. Note that this method does not account for the curvature of
	 * the earth (aka the Earth is flat).
	 *
	 * @return a Pair of Coordinates, with the upper-left and lower-right points
	 *         of this spatial range.
	 */
	public Pair<Coordinates, Coordinates> get2DCoordinates() {
		return new Pair<>(new Coordinates(this.getUpperBoundForLatitude(), this.getLowerBoundForLongitude()),
				new Coordinates(this.getLowerBoundForLatitude(), this.getUpperBoundForLongitude()));
	}
	
	public List<Coordinates> getPolygon(){
		return this.polygon;
	}

	public boolean hasElevationBounds() {
		return hasElevation;
	}

	public float getUpperBoundForElevation() {
		return upperElevation;
	}

	public float getLowerBoundForElevation() {
		return lowerElevation;
	}

	public boolean hasPolygon() {
		return this.polygon != null;
	}

	@Override
	public String toString() {
		Pair<Coordinates, Coordinates> p = get2DCoordinates();
		return "[" + p.a + ", " + p.b + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (hasElevation ? 1231 : 1237);
		result = prime * result + Float.floatToIntBits(lowerElevation);
		result = prime * result + Float.floatToIntBits(lowerLat);
		result = prime * result + Float.floatToIntBits(lowerLon);
		result = prime * result + Float.floatToIntBits(upperElevation);
		result = prime * result + Float.floatToIntBits(upperLat);
		result = prime * result + Float.floatToIntBits(upperLon);
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
		SpatialRange other = (SpatialRange) obj;
		if (hasElevation != other.hasElevation) {
			return false;
		}
		if (Float.floatToIntBits(lowerElevation) != Float.floatToIntBits(other.lowerElevation)) {
			return false;
		}
		if (Float.floatToIntBits(lowerLat) != Float.floatToIntBits(other.lowerLat)) {
			return false;
		}
		if (Float.floatToIntBits(lowerLon) != Float.floatToIntBits(other.lowerLon)) {
			return false;
		}
		if (Float.floatToIntBits(upperElevation) != Float.floatToIntBits(other.upperElevation)) {
			return false;
		}
		if (Float.floatToIntBits(upperLat) != Float.floatToIntBits(other.upperLat)) {
			return false;
		}
		if (Float.floatToIntBits(upperLon) != Float.floatToIntBits(other.upperLon)) {
			return false;
		}
		return true;
	}

	@Deserialize
	public SpatialRange(SerializationInputStream in) throws IOException, SerializationException {
		lowerLat = in.readFloat();
		upperLat = in.readFloat();
		lowerLon = in.readFloat();
		upperLon = in.readFloat();

		hasElevation = in.readBoolean();
		if (hasElevation) {
			lowerElevation = in.readFloat();
			upperElevation = in.readFloat();
		}
		
		boolean hasPolygon = in.readBoolean();
		if(hasPolygon){
			List<Coordinates> poly = new ArrayList<Coordinates>();
			in.readSerializableCollection(Coordinates.class, poly);
			polygon = poly;
		}
	}

	@Override
	public void serialize(SerializationOutputStream out) throws IOException {
		out.writeFloat(lowerLat);
		out.writeFloat(upperLat);
		out.writeFloat(lowerLon);
		out.writeFloat(upperLon);

		out.writeBoolean(hasElevation);
		if (hasElevation) {
			out.writeFloat(lowerElevation);
			out.writeFloat(upperElevation);
		}
		
		out.writeBoolean(hasPolygon());
		if(hasPolygon()){
			out.writeSerializableCollection(polygon);
		}
	}
}

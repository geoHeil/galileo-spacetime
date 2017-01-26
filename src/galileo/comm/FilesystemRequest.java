package galileo.comm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import galileo.dataset.SpatialHint;
import galileo.dataset.feature.FeatureType;
import galileo.event.Event;
import galileo.serialization.SerializationException;
import galileo.serialization.SerializationInputStream;
import galileo.serialization.SerializationOutputStream;
import galileo.util.GeoHash;
import galileo.util.Pair;

/**
 * For use by clients to create or delete file systems in galileo
 * 
 * @author kachikaran
 *
 */
public class FilesystemRequest implements Event {
	public static final int MAX_PRECISION = GeoHash.MAX_PRECISION/5;
	/**
	 * Name of the filesystem. 5-50 characters. Can include alphabets, numbers
	 * and hyphens
	 */
	private String name;

	/**
	 * FileSystemAction enum. Can be CREATE, DELETE or PERSIST
	 */
	private FilesystemAction action;

	/**
	 * The geohash precision that should be used to store the region-wise data.
	 * Range [2-FilesystemRequest.MAX_PRECISION]. Default is 4.
	 */
	private int precision;

	/**
	 * Temporal Type to use for the Network organization. Default is
	 * DAY_OF_MONTH. Can be YEAR, MONTH, DAY_OF_MONTH or HOUR_OF_DAY
	 */
	private TemporalType temporalType;

	/**
	 * The number of nodes in one group. The last group may not contain equal
	 * number as the rest depending on the number of nodes available. Default is
	 * 0 indicating the underlying network organization
	 */
	private int nodesPerGroup;

	/**
	 * The list featureName, featureType pairs such as Pair
	 * &lt;String,FeatureType&gt;("ch4", FeatureType.FLOAT)
	 */
	private List<Pair<String, FeatureType>> featureList;

	/**
	 * Should include a latitude and longitude hint which must be present in the
	 * feature list and their type should be of type FeatureType.FLOAT
	 */
	private SpatialHint spatialHint;

	/**
	 * @param name:
	 *            Name of the filesystem. 5-50 characters. Can include
	 *            alphabets, numbers and hyphens
	 * @param action:
	 *            FileSystemAction enum. Can be CREATE, DELETE or PERSIST
	 * @param featureList:
	 *            The list featureName, featureType pairs such as
	 *            Pair&lt;String,FeatureType&gt;("ch4", FeatureType.FLOAT)
	 * @param spatialHint:
	 *            Should include a latitude and longitude hint which must be
	 *            present in the feature list and their type should be of type
	 *            FeatureType.FLOAT
	 */
	public FilesystemRequest(String name, FilesystemAction action, List<Pair<String, FeatureType>> featureList,
			SpatialHint spatialHint) {
		if (name == null || name.trim().length() == 0 || !name.matches("[a-z0-9-]{5,50}"))
			throw new IllegalArgumentException(
					"name is required and must be lowercase having length at least 5 and at most 50 characters. "
							+ "alphabets, numbers and hyphens are allowed.");
		if (action == null)
			throw new IllegalArgumentException(
					"action cannot be null. must be one of the actions specified by galileo.comm.FileSystemAction");
		if (featureList != null && spatialHint == null)
			throw new IllegalArgumentException("Spatial hint is needed when feature list is provided");
		if (this.featureList != null && this.spatialHint != null) {
			boolean latOK = false;
			boolean lngOK = false;
			for (Pair<String, FeatureType> pair : this.featureList) {
				if (pair.a.equals(this.spatialHint.getLatitudeHint()) && pair.b == FeatureType.FLOAT)
					latOK = true;
				else if (pair.a.equals(this.spatialHint.getLongitudeHint()) && pair.b == FeatureType.FLOAT)
					lngOK = true;
			}
			if (!latOK)
				throw new IllegalArgumentException(
						"latitude hint must be one of the features in feature list and its type must be FeatureType.FLOAT");
			if (!lngOK)
				throw new IllegalArgumentException(
						"longitude hint must be one of the features in feature list and its type must be FeatureType.FLOAT");
		}
		this.name = name;
		this.precision = 4;
		this.nodesPerGroup = 0;
		this.temporalType = TemporalType.DAY_OF_MONTH;
		this.action = action;
		this.featureList = featureList;
		this.spatialHint = spatialHint;
	}

	public String getFeatures() {
		if (this.featureList == null)
			return null;
		StringBuffer sb = new StringBuffer();
		for (Pair<String, FeatureType> pair : this.featureList) {
			sb.append(pair.a + ":" + pair.b.toInt() + ",");
		}
		sb.setLength(sb.length() - 1);
		return sb.toString();
	}

	private boolean hasFeatures() {
		return this.featureList != null;
	}

	private boolean hasSpatialHint() {
		return this.spatialHint != null;
	}

	public List<Pair<String, FeatureType>> getFeatureList() {
		return this.featureList;
	}

	private List<Pair<String, FeatureType>> getFeatureList(String features) {
		if (features == null)
			return null;
		String[] pairs = features.split(",");
		this.featureList = new ArrayList<>();
		for (String pair : pairs) {
			String[] pairSplit = pair.split(":");
			this.featureList.add(
					new Pair<String, FeatureType>(pairSplit[0], FeatureType.fromInt(Integer.parseInt(pairSplit[1]))));
		}
		return this.featureList;
	}

	public SpatialHint getSpatialHint() {
		return this.spatialHint;
	}

	public void setPrecision(int precision) {
		if (precision >=2 && precision <= MAX_PRECISION)
			this.precision = precision;
	}

	public void setTemporalType(TemporalType temporalType) {
		if (temporalType != null) {
			this.temporalType = temporalType;
		}
	}

	public void setNodesPerGroup(int numNodes) {
		if (numNodes > 0)
			this.nodesPerGroup = numNodes;
	}

	public int getNodesPerGroup() {
		return this.nodesPerGroup;
	}

	public int getPrecision() {
		return this.precision;
	}

	public String getName() {
		return this.name;
	}

	public String getTemporalString() {
		return this.temporalType.name();
	}

	public int getTemporalValue() {
		return this.temporalType.getType();
	}

	public TemporalType getTemporalType() {
		return this.temporalType;
	}

	public FilesystemAction getAction() {
		return this.action;
	}

	@Deserialize
	public FilesystemRequest(SerializationInputStream in) throws IOException, SerializationException {
		this.name = in.readString();
		this.precision = in.readInt();
		this.action = FilesystemAction.fromAction(in.readString());
		this.temporalType = TemporalType.fromType(in.readInt());
		this.nodesPerGroup = in.readInt();
		if (in.readBoolean())
			this.featureList = getFeatureList(in.readString());
		if (in.readBoolean())
			this.spatialHint = new SpatialHint(in);
	}

	@Override
	public void serialize(SerializationOutputStream out) throws IOException {
		out.writeString(this.name);
		out.writeInt(this.precision);
		out.writeString(this.action.getAction());
		out.writeInt(this.temporalType.getType());
		out.writeInt(this.nodesPerGroup);
		out.writeBoolean(hasFeatures());
		if (hasFeatures())
			out.writeString(getFeatures());
		out.writeBoolean(hasSpatialHint());
		if (hasSpatialHint())
			this.spatialHint.serialize(out);

	}

}
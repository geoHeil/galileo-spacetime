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
 * Internal use only. To create or delete file systems in galileo
 * @author kachikaran
 *
 */
public class FilesystemEvent implements Event{

	public static final int MAX_PRECISION = GeoHash.MAX_PRECISION/5;
	private String name;
	private FilesystemAction action;
	private int precision;
	private TemporalType temporalType;
	private int nodesPerGroup;
	private List<Pair<String, FeatureType>> featureList;
	private SpatialHint spatialHint;

	public FilesystemEvent(String name, FilesystemAction action, List<Pair<String, FeatureType>> featureList,
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
		this.nodesPerGroup = 0; //zero indicates to make use of the default network organization
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
	
	public List<Pair<String, FeatureType>> getFeatureList(){
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
	public FilesystemEvent(SerializationInputStream in) throws IOException, SerializationException {
		this.name = in.readString();
		this.precision = in.readInt();
		this.action = FilesystemAction.fromAction(in.readString());
		this.temporalType = TemporalType.fromType(in.readInt());
		this.nodesPerGroup = in.readInt();
		if(in.readBoolean())
			this.featureList = getFeatureList(in.readString());
		if(in.readBoolean())
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
		if(hasSpatialHint())
			this.spatialHint.serialize(out);
			
	}
}
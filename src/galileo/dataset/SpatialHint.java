package galileo.dataset;

import java.io.IOException;

import galileo.serialization.ByteSerializable;
import galileo.serialization.SerializationInputStream;
import galileo.serialization.SerializationOutputStream;

public class SpatialHint implements ByteSerializable{
	
	private String latitudeHint;
	private String longitudeHint;
	
	public SpatialHint(String latHint, String longHint) {
		this.latitudeHint = latHint;
		this.longitudeHint = longHint;
	}
	
	public String getLatitudeHint(){
		return this.latitudeHint;
	}
	
	public String getLongitudeHint() {
		return this.longitudeHint;
	}
	
	@Deserialize
    public SpatialHint(SerializationInputStream in)
    throws IOException {
        this.latitudeHint = in.readString();
        this.longitudeHint = in.readString();
    }

	@Override
	public void serialize(SerializationOutputStream out) throws IOException {
		out.writeString(latitudeHint);
		out.writeString(longitudeHint);
	}

}

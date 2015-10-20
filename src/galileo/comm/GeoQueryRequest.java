package galileo.comm;

import galileo.dataset.Coordinates;
import galileo.event.Event;
import galileo.serialization.SerializationException;
import galileo.serialization.SerializationInputStream;
import galileo.serialization.SerializationOutputStream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GeoQueryRequest implements Event{
	
	private List<Coordinates> polygon;

    public GeoQueryRequest(List<Coordinates> polygon) {
        this.polygon = polygon;
    }

    public List<Coordinates> getPolygon() {
        return polygon;
    }
	
	@Deserialize
    public GeoQueryRequest(SerializationInputStream in)
    throws IOException, SerializationException {
		List<Coordinates> poly = new ArrayList<Coordinates>();
        in.readSerializableCollection(Coordinates.class, poly);
        this.polygon = poly;
    }

	@Override
	public void serialize(SerializationOutputStream out) throws IOException {
		out.writeSerializableCollection(polygon);
	}

}

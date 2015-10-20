package galileo.comm;

import galileo.dataset.Coordinates;
import galileo.event.Event;
import galileo.serialization.SerializationException;
import galileo.serialization.SerializationInputStream;
import galileo.serialization.SerializationOutputStream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * For use at the server side. Used by the storage node to query other storage nodes
 * @author jcharles
 */
public class GeoQueryEvent implements Event{
	
	private List<Coordinates> polygon;

    public GeoQueryEvent(List<Coordinates> polygon) {
        this.polygon = polygon;
    }

    public List<Coordinates> getPolygon() {
        return polygon;
    }
	
	@Deserialize
    public GeoQueryEvent(SerializationInputStream in)
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

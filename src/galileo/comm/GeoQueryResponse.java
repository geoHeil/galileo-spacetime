package galileo.comm;

import galileo.dataset.Metadata;
import galileo.event.Event;
import galileo.serialization.SerializationException;
import galileo.serialization.SerializationInputStream;
import galileo.serialization.SerializationOutputStream;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class GeoQueryResponse implements Event{
	
	private Set<Metadata> results;
	
	public GeoQueryResponse(Set<Metadata> results) {
        this.results = results;
    }
	
	public Set<Metadata> getResults() {
		return results;
	}
	
	@Deserialize
    public GeoQueryResponse(SerializationInputStream in)
    throws IOException, SerializationException {
        Set<Metadata> metadataSet = new HashSet<Metadata>();
        in.readSerializableCollection(Metadata.class, metadataSet);
        this.results = metadataSet;
    }

	@Override
	public void serialize(SerializationOutputStream out) throws IOException {
		out.writeSerializableCollection(results);
	}
	

}


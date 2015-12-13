package galileo.comm;

import galileo.dht.NodeInfo;
import galileo.event.Event;
import galileo.serialization.SerializationException;
import galileo.serialization.SerializationInputStream;
import galileo.serialization.SerializationOutputStream;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class GenericResponse implements Event{
	
	private GenericEventType eventType;
	private Object results;
	
	public GenericResponse(GenericEventType get, Object results) {
        this.eventType = get;
        this.results = results;
    }
	
	public GenericEventType getEventType() {
		return eventType;
	}

	public Object getResults() {
		return results;
	}
	
	@Deserialize
    public GenericResponse(SerializationInputStream in)
    throws IOException, SerializationException {
        String type = in.readString();
        for(GenericEventType gevent : GenericEventType.values()){
        	if(gevent.getType().equals(type)){
        		this.eventType = gevent;
        	}
        }
        if(this.eventType == GenericEventType.FEATURES){
        	Set<String> readResults = new HashSet<String>();
        	in.readStringCollection(readResults);
        	this.results = readResults;
        } else if(this.eventType == GenericEventType.NODEINFO){
        	Set<NodeInfo> readResults = new HashSet<NodeInfo>();
        	in.readSerializableCollection(NodeInfo.class, readResults);
        	this.results = readResults;
        } else if(this.eventType == GenericEventType.LOCALITY){
        	Map<String, String> readResults = new HashMap<String, String>();
        	in.readStringMap(readResults);
        	this.results = readResults;
        }
    }

	@SuppressWarnings("unchecked")
	@Override
	public void serialize(SerializationOutputStream out) throws IOException {
		out.writeString(eventType.getType());
		if(eventType == GenericEventType.FEATURES)
			out.writeStringCollection((Set<String>)results);
		else if(eventType == GenericEventType.NODEINFO)
			out.writeSerializableCollection((Set<NodeInfo>)results);
		else if(eventType == GenericEventType.LOCALITY)
			out.writeStringMap((Map<String, String>)results);
	}
	

}

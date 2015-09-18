package galileo.comm;

import galileo.event.Event;
import galileo.serialization.SerializationException;
import galileo.serialization.SerializationInputStream;
import galileo.serialization.SerializationOutputStream;

import java.io.IOException;

public class GenericRequest implements Event{
	
	private GenericEventType eventType;
	
	public GenericRequest(GenericEventType get) {
		this.eventType = get;
	}
	
	public GenericEventType getEventType(){
		return this.eventType;
	}
	
	@Deserialize
    public GenericRequest(SerializationInputStream in)
    throws IOException, SerializationException {
        String type = in.readString();
        for(GenericEventType gevent : GenericEventType.values()){
        	if(gevent.getType().equals(type)){
        		this.eventType = gevent;
        	}
        }
    }

	@Override
	public void serialize(SerializationOutputStream out) throws IOException {
		out.writeString(eventType.getType());
	}

}

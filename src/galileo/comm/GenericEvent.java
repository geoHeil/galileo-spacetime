package galileo.comm;


import galileo.event.Event;
import galileo.serialization.SerializationException;
import galileo.serialization.SerializationInputStream;
import galileo.serialization.SerializationOutputStream;

import java.io.IOException;

/**
 * A server side request event corresponding to the client side request event.
 * @author jcharles
 *
 */
public class GenericEvent implements Event{
	
	private GenericEventType eventType;
	
	public GenericEvent(GenericEventType get) {
		this.eventType = get;
	}
	
	public GenericEventType getEventType(){
		return this.eventType;
	}
	
	@Deserialize
    public GenericEvent(SerializationInputStream in)
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

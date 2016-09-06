package galileo.comm;

import galileo.event.Event;
import galileo.serialization.SerializationException;
import galileo.serialization.SerializationInputStream;
import galileo.serialization.SerializationOutputStream;

import java.io.IOException;

import org.json.JSONObject;

/**
 * A client interface to request meta information from galileo such as the file system names and features in a filesystem.
 * <br/>Request must be a JSON string in the following format:<br/>
 * { "kind" : "galileo#filesystem" | "galileo#features",<br/>
 * &nbsp;&nbsp;"filesystem" : ["Array of Strings indicating the names of the filesystem" - required if the kind is galileo#features"], <br/>
 * }<br/>
 * The response would be an instance of {@link MetaResponse}
 * @author jkachika
 */
public class MetaRequest implements Event{
	
	private JSONObject request;
	
	public MetaRequest(String reqJSON){
		if(reqJSON == null || reqJSON.trim().length() == 0)
			throw new IllegalArgumentException("Request must be a valid JSON string.");
		this.request = new JSONObject(reqJSON);
	}
	
	public MetaRequest(JSONObject request) {
		if(request == null) 
			throw new IllegalArgumentException("Request must be a valid JSON object.");
		this.request = request;
	}
	
	public String getRequestString(){
		return this.request.toString();
	}
	
	public JSONObject getRequest(){
		return this.request;
	}
	
	@Deserialize
    public MetaRequest(SerializationInputStream in)
    throws IOException, SerializationException {
        String reqJSON = in.readString();
        this.request = new JSONObject(reqJSON);
    }

	@Override
	public void serialize(SerializationOutputStream out) throws IOException {
		out.writeString(this.request.toString());
	}

}

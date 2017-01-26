package galileo.comm;

import java.io.IOException;

import org.json.JSONObject;

import galileo.event.Event;
import galileo.serialization.SerializationException;
import galileo.serialization.SerializationInputStream;
import galileo.serialization.SerializationOutputStream;

public class MetadataResponse implements Event{
	
	private JSONObject response;
	
	public MetadataResponse(JSONObject response){
		this.response = response;
	}
	
	public MetadataResponse(String jsonResponse){
		this.response = new JSONObject(jsonResponse);
	}
	
	public String getResponseString(){
		return this.response.toString();
	}
	
	public JSONObject getResponse(){
		return this.response;
	}
	
	@Deserialize
    public MetadataResponse(SerializationInputStream in)
    throws IOException, SerializationException {
        String jsonResponse = in.readString();
        this.response = new JSONObject(jsonResponse);
    }

	@Override
	public void serialize(SerializationOutputStream out) throws IOException {
		out.writeString(this.response.toString());
	}
	

}

package galileo.comm;

/**
 * Helps to identify the type of the generic event. The following are the supported types: <br/>
 * <i> FEATURES - </i> List of Features as a list of strings
 * 
 * @author jkachika
 */
public enum GenericEventType {
	FEATURES("features"), NODEINFO("nodeinfo");
	
	private String type;
	
	private GenericEventType(String type){
		this.type = type;
	}
	
	public String getType(){
		return this.type;
	}
}

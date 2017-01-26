package galileo.comm;

public enum FilesystemAction {
	CREATE("create"), DELETE("delete"), PERSIST("persist");
	
	private String action;
	private FilesystemAction(String action){
		this.action = action;
	}
	
	public String getAction(){
		return this.action;
	}
	
	public static FilesystemAction fromAction(String action){
		for(FilesystemAction fsa: FilesystemAction.values())
			if(fsa.getAction().equalsIgnoreCase(action))
				return fsa;
		throw new IllegalArgumentException("No such action is supported");
	}
}

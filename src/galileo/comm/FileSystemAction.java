package galileo.comm;

public enum FileSystemAction {
	CREATE("create"), DELETE("delete");
	
	private String action;
	private FileSystemAction(String action){
		this.action = action;
	}
	
	public String getAction(){
		return this.action;
	}
	
	public static FileSystemAction fromAction(String action){
		for(FileSystemAction fsa: FileSystemAction.values())
			if(fsa.getAction().equalsIgnoreCase(action))
				return fsa;
		throw new IllegalArgumentException("No such action is supported");
	}
}

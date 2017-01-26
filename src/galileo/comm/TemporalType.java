package galileo.comm;

import java.util.Calendar;

public enum TemporalType {
	YEAR(Calendar.YEAR), MONTH(Calendar.MONTH), DAY_OF_MONTH(Calendar.DAY_OF_MONTH), HOUR_OF_DAY(Calendar.HOUR_OF_DAY);
	
	private int type;
	
	private TemporalType(int type){
		this.type = type;
	}
	
	public int getType(){
		return this.type;
	}
	
	public static TemporalType fromType(int type){
		for(TemporalType tType : TemporalType.values())
			if (tType.getType() == type)
				return tType;
		return TemporalType.DAY_OF_MONTH;
	}
}

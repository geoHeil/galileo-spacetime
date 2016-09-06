package galileo.util;

public class Math {
	public static Float getFloat(String number) {
		try {
			return Float.parseFloat(number);
		} catch (Exception e) {
			return Float.NaN;
		}
	}
	
	public static Integer getInteger(String number){
		try {
			return Integer.parseInt(number);
		} catch (Exception e) {
			return 0;
		}
	}
	
	public static Long getLong(String number){
		try {
			return Long.parseLong(number);
		} catch (Exception e) {
			return 0l;
		}
	}
	
	public static Double getDouble(String number){
		try {
			return Double.parseDouble(number);
		} catch (Exception e) {
			return Double.NaN;
		}
	}
}

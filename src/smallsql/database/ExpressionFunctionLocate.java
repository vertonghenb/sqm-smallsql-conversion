package smallsql.database;
public class ExpressionFunctionLocate extends ExpressionFunctionReturnInt {
	int getFunction() {
		return SQLTokenizer.LOCATE;
	}
	boolean isNull() throws Exception {
		return param1.isNull() || param2.isNull();
	}
	int getInt() throws Exception {
		String suchstr = param1.getString();
		String value   = param2.getString();
		if(suchstr == null || value == null || suchstr.length() == 0 || value.length() == 0) return 0;
		int start = 0;
		if(param3 != null){
			start = param3.getInt()-1;
		}
		return value.toUpperCase().indexOf( suchstr.toUpperCase(), start ) +1;
	}
}
package smallsql.database;
public class ExpressionFunctionChar extends ExpressionFunctionReturnString {
	final int getFunction() {
		return SQLTokenizer.CHAR;
	}
    final String getString() throws Exception {
		if(isNull()) return null;
		char chr = (char)param1.getInt();
		return String.valueOf(chr);
	}
	final int getDataType() {
		return SQLTokenizer.CHAR;
	}
	final int getPrecision(){
		return 1;
	}
}
package smallsql.database;
final class ExpressionFunctionCharLen extends ExpressionFunctionReturnInt {
	final int getFunction() {
		return SQLTokenizer.CHARLEN;
	}
    boolean isNull() throws Exception {
        return param1.isNull();
    }
	final int getInt() throws Exception {
        if(isNull()) return 0;
        String str = param1.getString();
		return str.length();
	}
}
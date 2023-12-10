package smallsql.database;
final class ExpressionFunctionOctetLen extends ExpressionFunctionReturnInt {
	private static final int BYTES_PER_CHAR = 2;
	final int getFunction() {
		return SQLTokenizer.OCTETLEN;
	}
    boolean isNull() throws Exception {
        return param1.isNull();
    }
	final int getInt() throws Exception {
        if(isNull()) return 0;
        String str = param1.getString();
		return str.length() * BYTES_PER_CHAR;
	}
}
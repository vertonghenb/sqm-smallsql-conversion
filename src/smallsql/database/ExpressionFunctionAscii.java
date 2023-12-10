package smallsql.database;
final class ExpressionFunctionAscii extends ExpressionFunctionReturnInt {
	final int getFunction() {
		return SQLTokenizer.ASCII;
	}
	final boolean isNull() throws Exception {
		return param1.isNull() || param1.getString().length() == 0;
	}
	final int getInt() throws Exception {
		String str = param1.getString();
		if(str == null || str.length() == 0) return 0;
		return str.charAt(0);
	}
	final Object getObject() throws Exception {
		String str = param1.getString();
		if(str == null || str.length() == 0) return null;
		return Utils.getInteger(str.charAt(0));
	}
}
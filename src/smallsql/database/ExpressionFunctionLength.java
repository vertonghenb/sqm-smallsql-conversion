package smallsql.database;
final class ExpressionFunctionLength extends ExpressionFunctionReturnInt {
	final int getFunction() {
		return SQLTokenizer.LENGTH;
	}
	final int getInt() throws Exception {
		String str = param1.getString();
		if(str == null) return 0;
		int length = str.length();
		while(length>=0 && str.charAt(length-1) == ' ') length--;
		return length;
	}
}
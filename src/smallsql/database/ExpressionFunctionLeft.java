package smallsql.database;
public class ExpressionFunctionLeft extends ExpressionFunctionReturnP1StringAndBinary {
	final int getFunction() {
		return SQLTokenizer.LEFT;
	}
	final boolean isNull() throws Exception {
		return param1.isNull() || param2.isNull();
	}
	final byte[] getBytes() throws Exception{
		if(isNull()) return null;
		byte[] bytes = param1.getBytes();
		int length = param2.getInt();
		if(bytes.length <= length) return bytes;
		byte[] b = new byte[length];
		System.arraycopy(bytes, 0, b, 0, length);
		return b;		
	}
	final String getString() throws Exception {
		if(isNull()) return null;
		String str = param1.getString();
		int length = param2.getInt();
		length = Math.min( length, str.length() );
		return str.substring(0,length);
	}
}
package smallsql.database;
public class ExpressionFunctionLTrim extends ExpressionFunctionReturnP1StringAndBinary {
	final int getFunction() {
		return SQLTokenizer.LTRIM;
	}
	final boolean isNull() throws Exception {
		return param1.isNull();
	}
	final byte[] getBytes() throws Exception{
		if(isNull()) return null;
		byte[] bytes = param1.getBytes();
        int start = 0;
        int length = bytes.length;
        while(start<length && bytes[start]==0){
            start++;
        }
        length -= start; 
		byte[] b = new byte[length];
		System.arraycopy(bytes, start, b, 0, length);
		return b;		
	}
	final String getString() throws Exception {
		if(isNull()) return null;
		String str = param1.getString();
        int start = 0;
        while(start<str.length() && str.charAt(start)==' '){
            start++;
        }
		return str.substring(start);
	}
}
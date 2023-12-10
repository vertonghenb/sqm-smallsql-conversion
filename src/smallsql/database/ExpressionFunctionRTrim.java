package smallsql.database;
public class ExpressionFunctionRTrim extends ExpressionFunctionReturnP1StringAndBinary {
	final int getFunction() {
		return SQLTokenizer.RTRIM;
	}
	final boolean isNull() throws Exception {
		return param1.isNull();
	}
	final byte[] getBytes() throws Exception{
		if(isNull()) return null;
		byte[] bytes = param1.getBytes();
        int length = bytes.length;
        while(length>0 && bytes[length-1]==0){
            length--;
        }
		byte[] b = new byte[length];
		System.arraycopy(bytes, 0, b, 0, length);
		return b;		
	}
	final String getString() throws Exception {
		if(isNull()) return null;
		String str = param1.getString();
        int length = str.length();
        while(length>0 && str.charAt(length-1)==' '){
            length--;
        }
		return str.substring(0,length);
	}
}
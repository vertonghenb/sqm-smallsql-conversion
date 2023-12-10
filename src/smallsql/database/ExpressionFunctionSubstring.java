package smallsql.database;
import smallsql.database.language.Language;
final class ExpressionFunctionSubstring extends ExpressionFunctionReturnP1StringAndBinary {
	final int getFunction() {
		return SQLTokenizer.SUBSTRING;
	}
	final boolean isNull() throws Exception {
		return param1.isNull() || param2.isNull() || param3.isNull();
	}
	final byte[] getBytes() throws Exception{
		if(isNull()) return null;
		byte[] bytes = param1.getBytes();
		int byteLen = bytes.length;
		int start  = Math.min( Math.max( 0, param2.getInt() - 1), byteLen);
		int length = param3.getInt();
		if(length < 0) 
			throw SmallSQLException.create(Language.SUBSTR_INVALID_LEN, new Integer(length));
		if(start == 0 && byteLen == length) return bytes;
		if(byteLen > length + start){
			byte[] b = new byte[length];
			System.arraycopy(bytes, start, b, 0, length);
			return b;		
		}else{
			byte[] b = new byte[byteLen - start];
			System.arraycopy(bytes, start, b, 0, b.length);
			return b;		
		}
	}
	final String getString() throws Exception {
		if(isNull()) return null;
		String str = param1.getString();
		int strLen = str.length();
		int start  = Math.min( Math.max( 0, param2.getInt() - 1), strLen);
		int length = param3.getInt();
		if(length < 0) 
			throw SmallSQLException.create(Language.SUBSTR_INVALID_LEN, new Integer(length));
		length = Math.min( length, strLen-start );
		return str.substring(start, start+length);
	}
}
package smallsql.database;
import java.io.ByteArrayOutputStream;
import smallsql.database.language.Language;
public class ExpressionFunctionInsert extends ExpressionFunctionReturnP1StringAndBinary {
	final int getFunction() {
		return SQLTokenizer.INSERT;
	}
	final boolean isNull() throws Exception {
		return param1.isNull() || param2.isNull() || param3.isNull() || param4.isNull();
	}
	final byte[] getBytes() throws Exception{
        if(isNull()) return null;
        byte[] bytes = param1.getBytes();
        int start  = Math.min(Math.max( 0, param2.getInt() - 1), bytes.length );
        int length = Math.min(param3.getInt(), bytes.length );
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        buffer.write(bytes,0,start);
        buffer.write(param4.getBytes());
        if(length < 0) 
            throw SmallSQLException.create(Language.INSERT_INVALID_LEN, new Integer(length));
        buffer.write(bytes, start+length, bytes.length-start-length);
        return buffer.toByteArray();
	}
	final String getString() throws Exception {
		if(isNull()) return null;
		String str = param1.getString();
        int start  = Math.min(Math.max( 0, param2.getInt() - 1), str.length() );
		int length = Math.min(param3.getInt(), str.length() );
        StringBuffer buffer = new StringBuffer();
        buffer.append(str.substring(0,start));
        buffer.append(param4.getString());
        if(length < 0) 
            throw SmallSQLException.create(Language.INSERT_INVALID_LEN, new Integer(length));
        buffer.append(str.substring(start+length));
		return buffer.toString();
	}
    int getPrecision() {
        return param1.getPrecision()+param2.getPrecision();
    }
}
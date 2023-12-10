package smallsql.database;
import java.io.ByteArrayOutputStream;
public class ExpressionFunctionRepeat extends ExpressionFunctionReturnP1StringAndBinary {
	final int getFunction() {
		return SQLTokenizer.REPEAT;
	}
	final byte[] getBytes() throws Exception{
        if(isNull()) return null;
        byte[] bytes = param1.getBytes();
        int count  = param2.getInt();
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        for(int i=0; i<count; i++){
            buffer.write(bytes);
        }
        return buffer.toByteArray();
	}
	final String getString() throws Exception {
		if(isNull()) return null;
		String str = param1.getString();
        int count  = param2.getInt();
        StringBuffer buffer = new StringBuffer();
        for(int i=0; i<count; i++){
            buffer.append(str);
        }
		return buffer.toString();
	}
    int getPrecision() {
        return SSResultSetMetaData.getDataTypePrecision( getDataType(), -1 );
    }
}
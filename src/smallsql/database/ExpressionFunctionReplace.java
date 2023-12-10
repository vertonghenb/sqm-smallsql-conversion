package smallsql.database;
import java.io.ByteArrayOutputStream;
public class ExpressionFunctionReplace extends ExpressionFunctionReturnP1StringAndBinary {
	final int getFunction() {
		return SQLTokenizer.REPLACE;
	}
	final boolean isNull() throws Exception {
		return param1.isNull() || param2.isNull() || param3.isNull();
	}
	final byte[] getBytes() throws Exception{
		if(isNull()) return null;
        byte[] str1 = param1.getBytes();
        byte[] str2  = param2.getBytes();
        int length = str2.length;
        if(length == 0){
            return str1;
        }
        byte[] str3  = param3.getBytes();
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int idx1 = 0;
        int idx2 = Utils.indexOf(str2,str1,idx1);
        while(idx2 > 0){
            buffer.write(str1,idx1,idx2-idx1);
            buffer.write(str3);
            idx1 = idx2 + length;
            idx2 = Utils.indexOf(str2,str1,idx1);
        }
        if(idx1 > 0){
            buffer.write(str1,idx1,str1.length-idx1);
            return buffer.toByteArray();
        }
        return str1;
	}
	final String getString() throws Exception {
		if(isNull()) return null;
		String str1 = param1.getString();
		String str2  = param2.getString();
        int length = str2.length();
        if(length == 0){
            return str1;
        }
        String str3  = param3.getString();
        StringBuffer buffer = new StringBuffer();
        int idx1 = 0;
        int idx2 = str1.indexOf(str2,idx1);
        while(idx2 >= 0){
            buffer.append(str1.substring(idx1,idx2));
            buffer.append(str3);
            idx1 = idx2 + length;
            idx2 = str1.indexOf(str2,idx1);
        }
        if(idx1 > 0){
            buffer.append(str1.substring(idx1));
            return buffer.toString();
        }
		return str1;
	}
    int getPrecision() {
        return SSResultSetMetaData.getDataTypePrecision( getDataType(), -1 );
    }
}
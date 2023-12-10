package smallsql.database;
public class ExpressionFunctionLCase extends ExpressionFunctionReturnP1StringAndBinary {
	final int getFunction() {
		return SQLTokenizer.LCASE;
	}
	final boolean isNull() throws Exception {
		return param1.isNull();
	}
	final byte[] getBytes() throws Exception{
        if(isNull()) return null;
        return getString().getBytes();
	}
	final String getString() throws Exception {
		if(isNull()) return null;
		return param1.getString().toLowerCase();
	}
}
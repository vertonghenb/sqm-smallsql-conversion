package smallsql.database;
public class ExpressionFunctionSpace extends ExpressionFunctionReturnString {
	final int getFunction() {
		return SQLTokenizer.SPACE;
	}
    boolean isNull() throws Exception {
        return param1.isNull() || param1.getInt()<0;
    }
    final String getString() throws Exception {
		if(isNull()) return null;
        int size = param1.getInt();
        if(size < 0){
            return null;
        }
		char[] buffer = new char[size];
        for(int i=0; i<size; i++){
            buffer[i] = ' ';
        }
		return new String(buffer);
	}
	final int getDataType() {
		return SQLTokenizer.VARCHAR;
	}
}
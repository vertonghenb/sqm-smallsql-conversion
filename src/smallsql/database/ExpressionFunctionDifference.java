package smallsql.database;
final class ExpressionFunctionDifference extends ExpressionFunctionReturnInt {
	final int getFunction() {
		return SQLTokenizer.DIFFERENCE;
	}
    boolean isNull() throws Exception {
        return param1.isNull() || param2.isNull();
    }
	final int getInt() throws Exception {
        if(isNull()) return 0;
		String str1 = ExpressionFunctionSoundex.getString(param1.getString());
        String str2 = ExpressionFunctionSoundex.getString(param2.getString());
        int diff = 0;
        for(int i=0; i<4; i++){
            if(str1.charAt(i) == str2.charAt(i)){
                diff++;
            }
        }
		return diff;
	}
}
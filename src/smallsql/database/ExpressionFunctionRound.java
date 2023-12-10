package smallsql.database;
final class ExpressionFunctionRound extends ExpressionFunctionReturnP1Number {
    final int getFunction(){ return SQLTokenizer.ROUND; }
    boolean isNull() throws Exception{
        return param1.isNull() || param2.isNull();
    }
    final double getDouble() throws Exception{
		if(isNull()) return 0;
		final int places = param2.getInt();
		double value = param1.getDouble();
		long factor = 1;
		if(places > 0){
			for(int i=0; i<places; i++){
				factor *= 10;
			}
			value *= factor;
		}else{
			for(int i=0; i>places; i--){
				factor *= 10;
			}
			value /= factor;
		}
		value = Math.rint( value );
		if(places > 0){
			value /= factor;
		}else{
			value *= factor;
		}
		return value;
    }
}
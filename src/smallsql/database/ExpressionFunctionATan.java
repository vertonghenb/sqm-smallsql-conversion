package smallsql.database;
final class ExpressionFunctionATan extends ExpressionFunctionReturnFloat {
    final int getFunction(){ return SQLTokenizer.ATAN; }
    final double getDouble() throws Exception{
		if(isNull()) return 0;
        return Math.atan( param1.getDouble() );
    }
}
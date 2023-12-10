package smallsql.database;
final class ExpressionFunctionSqrt extends ExpressionFunctionReturnFloat {
    final int getFunction(){ return SQLTokenizer.SQRT; }
    final double getDouble() throws Exception{
		if(isNull()) return 0;
        return Math.sqrt( param1.getDouble() );
    }
}
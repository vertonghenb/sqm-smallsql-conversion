package smallsql.database;
final class ExpressionFunctionCot extends ExpressionFunctionReturnFloat {
    final int getFunction(){ return SQLTokenizer.COT; }
    final double getDouble() throws Exception{
		if(isNull()) return 0;
        return 1/Math.tan( param1.getDouble() );
    }
}
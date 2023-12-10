package smallsql.database;
final class ExpressionFunctionTan extends ExpressionFunctionReturnFloat {
    final int getFunction(){ return SQLTokenizer.TAN; }
    final double getDouble() throws Exception{
		if(isNull()) return 0;
        return Math.tan( param1.getDouble() );
    }
}
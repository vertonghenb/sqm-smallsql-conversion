package smallsql.database;
final class ExpressionFunctionCos extends ExpressionFunctionReturnFloat {
    final int getFunction(){ return SQLTokenizer.COS; }
    final double getDouble() throws Exception{
		if(isNull()) return 0;
        return Math.cos( param1.getDouble() );
    }
}
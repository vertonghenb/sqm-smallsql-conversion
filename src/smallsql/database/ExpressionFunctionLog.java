package smallsql.database;
final class ExpressionFunctionLog extends ExpressionFunctionReturnFloat {
    final int getFunction(){ return SQLTokenizer.LOG; }
    final double getDouble() throws Exception{
		if(isNull()) return 0;
        return Math.log( param1.getDouble() );
    }
}
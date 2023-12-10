package smallsql.database;
final class ExpressionFunctionExp extends ExpressionFunctionReturnFloat {
    final int getFunction(){ return SQLTokenizer.EXP; }
    final double getDouble() throws Exception{
		if(isNull()) return 0;
        return Math.exp( param1.getDouble() );
    }
}
package smallsql.database;
final class ExpressionFunctionLog10 extends ExpressionFunctionReturnFloat {
    final int getFunction(){ return SQLTokenizer.LOG10; }
    final double getDouble() throws Exception{
		if(isNull()) return 0;
        return Math.log( param1.getDouble() ) / divisor;
    }
	private static final double divisor = Math.log(10);
}
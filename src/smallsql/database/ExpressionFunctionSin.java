package smallsql.database;
final class ExpressionFunctionSin extends ExpressionFunctionReturnFloat {
    final int getFunction(){ return SQLTokenizer.SIN; }
    final double getDouble() throws Exception{
		if(isNull()) return 0;
        return Math.sin( param1.getDouble() );
    }
}
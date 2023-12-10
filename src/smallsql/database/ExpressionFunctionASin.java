package smallsql.database;
final class ExpressionFunctionASin extends ExpressionFunctionReturnFloat {
    final int getFunction(){ return SQLTokenizer.ASIN; }
    final double getDouble() throws Exception{
		if(isNull()) return 0;
        return Math.asin( param1.getDouble() );
    }
}
package smallsql.database;
final class ExpressionFunctionCeiling extends ExpressionFunctionReturnFloat {
    final int getFunction(){ return SQLTokenizer.CEILING; }
    final double getDouble() throws Exception{
		if(isNull()) return 0;
        return Math.ceil( param1.getDouble() );
    }
}
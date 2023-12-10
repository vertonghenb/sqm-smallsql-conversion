package smallsql.database;
final class ExpressionFunctionRadians extends ExpressionFunctionReturnFloat {
    final int getFunction(){ return SQLTokenizer.RADIANS; }
    final double getDouble() throws Exception{
		if(isNull()) return 0;
        return Math.toRadians( param1.getDouble() );
    }
}
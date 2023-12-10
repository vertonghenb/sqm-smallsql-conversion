package smallsql.database;
final class ExpressionFunctionATan2 extends ExpressionFunctionReturnFloat {
    final int getFunction(){ return SQLTokenizer.ATAN2; }
    boolean isNull() throws Exception{
        return param1.isNull() || param2.isNull();
    }
    final double getDouble() throws Exception{
		if(isNull()) return 0;
        return Math.atan2( param1.getDouble(), param2.getDouble() );
    }
}
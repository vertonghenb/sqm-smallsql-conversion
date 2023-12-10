package smallsql.database;
final class ExpressionFunctionACos extends ExpressionFunctionReturnFloat {
    final int getFunction(){ return SQLTokenizer.ACOS; }
    final double getDouble() throws Exception{
		if(isNull()) return 0;
        return Math.acos( param1.getDouble() );
    }
}
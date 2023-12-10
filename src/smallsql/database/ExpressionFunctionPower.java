package smallsql.database;
final class ExpressionFunctionPower extends ExpressionFunctionReturnFloat {
    final int getFunction(){ return SQLTokenizer.POWER; }
    boolean isNull() throws Exception{
        return param1.isNull() || param2.isNull();
    }
    final double getDouble() throws Exception{
		if(isNull()) return 0;
        return Math.pow( param1.getDouble(), param2.getDouble() );
    }
}
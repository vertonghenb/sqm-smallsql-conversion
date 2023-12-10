package smallsql.database;
final class ExpressionFunctionMod extends ExpressionFunctionReturnInt {
    final int getFunction(){ return SQLTokenizer.MOD; }
    boolean isNull() throws Exception{
        return param1.isNull() || param2.isNull();
    }
    final int getInt() throws Exception{
		if(isNull()) return 0;
        return param1.getInt() % param2.getInt();
    }
}
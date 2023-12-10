package smallsql.database;
final class ExpressionFunctionDegrees extends ExpressionFunctionReturnFloat {
    final int getFunction(){ return SQLTokenizer.DEGREES; }
    final double getDouble() throws Exception{
		if(isNull()) return 0;
        return Math.toDegrees( param1.getDouble() );
    }
}
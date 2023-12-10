package smallsql.database;
final class ExpressionFunctionPI extends ExpressionFunctionReturnFloat {
    final int getFunction(){ return SQLTokenizer.PI; }
    boolean isNull() throws Exception{
        return false;
    }
    final double getDouble() throws Exception{
        return Math.PI;
    }
}
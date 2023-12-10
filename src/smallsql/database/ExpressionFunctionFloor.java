package smallsql.database;
class ExpressionFunctionFloor extends ExpressionFunctionReturnP1Number {
    int getFunction(){ return SQLTokenizer.FLOOR; }
    double getDouble() throws Exception{
        return Math.floor( param1.getDouble() );
    }
    String getString() throws Exception{
        Object obj = getObject();
        if(obj == null) return null;
        return obj.toString();
    }
}
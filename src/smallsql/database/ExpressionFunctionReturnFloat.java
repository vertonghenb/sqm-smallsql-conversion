package smallsql.database;
abstract class ExpressionFunctionReturnFloat extends ExpressionFunction {
    boolean isNull() throws Exception{
        return param1.isNull();
    }
    final boolean getBoolean() throws Exception{
        return getDouble() != 0;
    }
	final int getInt() throws Exception{
        return (int)getDouble();
    }
	final long getLong() throws Exception{
        return (long)getDouble();
    }
	final float getFloat() throws Exception{
        return (float)getDouble();
    }
    long getMoney() throws Exception{
        return Utils.doubleToMoney(getDouble());
    }
	final MutableNumeric getNumeric() throws Exception{
		if(isNull()) return null;
		double value = getDouble();
		if(Double.isInfinite(value) || Double.isNaN(value))
			return null;
		return new MutableNumeric(value);
    }
	final Object getObject() throws Exception{
		if(isNull()) return null;
		return new Double(getDouble());
    }
	final String getString() throws Exception{
        Object obj = getObject();
        if(obj == null) return null;
        return obj.toString();
    }
	final int getDataType() {
		return SQLTokenizer.FLOAT;
	}
}
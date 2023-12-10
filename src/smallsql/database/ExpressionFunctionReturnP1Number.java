package smallsql.database;
abstract class ExpressionFunctionReturnP1Number extends ExpressionFunctionReturnP1 {
    final boolean getBoolean() throws Exception{
        return getDouble() != 0;
    }
	final int getInt() throws Exception {
		return Utils.long2int(getLong());
	}
    final long getLong() throws Exception{
        return Utils.double2long(getDouble());
    }
	final float getFloat() throws Exception {
		return (float)getDouble();
	}
    MutableNumeric getNumeric() throws Exception{
		if(param1.isNull()) return null;
		switch(getDataType()){
			case SQLTokenizer.INT:
				return new MutableNumeric(getInt());
			case SQLTokenizer.BIGINT:
				return new MutableNumeric(getLong());
			case SQLTokenizer.MONEY:
				return new MutableNumeric(getMoney(), 4);
			case SQLTokenizer.DECIMAL:
				MutableNumeric num = param1.getNumeric();
				num.floor();
				return num;
			case SQLTokenizer.DOUBLE:
				return new MutableNumeric(getDouble());
			default:
				throw new Error();
		}
    }
    long getMoney() throws Exception{
        return Utils.doubleToMoney(getDouble());
    }
	String getString() throws Exception {
		if(isNull()) return null;
		return getObject().toString();
	}
	final int getDataType() {
		return ExpressionArithmetic.getBestNumberDataType(param1.getDataType());
	}
}
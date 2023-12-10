package smallsql.database;
abstract class ExpressionFunctionReturnInt extends ExpressionFunction {
	boolean isNull() throws Exception {
		return param1.isNull();
	}
	final boolean getBoolean() throws Exception {
		return getInt() != 0;
	}
	final long getLong() throws Exception {
		return getInt();
	}
	final float getFloat() throws Exception {
		return getInt();
	}
	final double getDouble() throws Exception {
		return getInt();
	}
	final long getMoney() throws Exception {
		return getInt() * 10000;
	}
	final MutableNumeric getNumeric() throws Exception {
		if(isNull()) return null;
		return new MutableNumeric(getInt());
	}
	Object getObject() throws Exception {
		if(isNull()) return null;
		return Utils.getInteger(getInt());
	}
	final String getString() throws Exception {
		if(isNull()) return null;
		return String.valueOf(getInt());
	}
	final int getDataType() {
		return SQLTokenizer.INT;
	}
}
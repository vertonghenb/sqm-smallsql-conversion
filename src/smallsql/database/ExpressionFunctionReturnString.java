package smallsql.database;
abstract class ExpressionFunctionReturnString extends ExpressionFunction {
    boolean isNull() throws Exception {
		return param1.isNull();
	}
	final boolean getBoolean() throws Exception {
		if(isNull()) return false;
		return Utils.string2boolean(getString().trim());
	}
	final int getInt() throws Exception {
		if(isNull()) return 0;
		return Integer.parseInt(getString().trim());
	}
	final long getLong() throws Exception {
		if(isNull()) return 0;
		return Long.parseLong(getString().trim());
	}
	final float getFloat() throws Exception {
		if(isNull()) return 0;
		return Float.parseFloat(getString().trim());
	}
	final double getDouble() throws Exception {
		if(isNull()) return 0;
		return Double.parseDouble(getString().trim());
	}
	final long getMoney() throws Exception {
		if(isNull()) return 0;
		return Money.parseMoney(getString().trim());
	}
	final MutableNumeric getNumeric() throws Exception {
		if(isNull()) return null;
		return new MutableNumeric(getString().trim());
	}
	final Object getObject() throws Exception {
		return getString();
	}
}
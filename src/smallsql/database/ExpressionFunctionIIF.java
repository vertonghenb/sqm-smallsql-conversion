package smallsql.database;
final class ExpressionFunctionIIF extends ExpressionFunction {
	int getFunction() {
		return SQLTokenizer.IIF;
	}
	boolean isNull() throws Exception {
		if(param1.getBoolean())
			return param2.isNull();
		return param3.isNull();
	}
	boolean getBoolean() throws Exception {
		if(param1.getBoolean())
			return param2.getBoolean();
		return param3.getBoolean();
	}
	int getInt() throws Exception {
		if(param1.getBoolean())
			return param2.getInt();
		return param3.getInt();
	}
	long getLong() throws Exception {
		if(param1.getBoolean())
			return param2.getLong();
		return param3.getLong();
	}
	float getFloat() throws Exception {
		if(param1.getBoolean())
			return param2.getFloat();
		return param3.getFloat();
	}
	double getDouble() throws Exception {
		if(param1.getBoolean())
			return param2.getDouble();
		return param3.getDouble();
	}
	long getMoney() throws Exception {
		if(param1.getBoolean())
			return param2.getMoney();
		return param3.getMoney();
	}
	MutableNumeric getNumeric() throws Exception {
		if(param1.getBoolean())
			return param2.getNumeric();
		return param3.getNumeric();
	}
	Object getObject() throws Exception {
		if(param1.getBoolean())
			return param2.getObject();
		return param3.getObject();
	}
	String getString() throws Exception {
		if(param1.getBoolean())
			return param2.getString();
		return param3.getString();
	}
	final int getDataType() {
		return ExpressionArithmetic.getDataType(param2, param3);
	}
	final int getPrecision(){
		return Math.max( param2.getPrecision(), param3.getPrecision() );
	}
	final int getScale(){
		return Math.max( param2.getScale(), param3.getScale() );
	}
}
package smallsql.database;
class ExpressionFunctionAbs extends ExpressionFunctionReturnP1 {
    int getFunction(){ return SQLTokenizer.ABS; }
	boolean getBoolean() throws Exception{
        return getDouble() != 0;
    }
    int getInt() throws Exception{
        return Math.abs( param1.getInt() );
    }
    long getLong() throws Exception{
        return Math.abs( param1.getLong() );
    }
    float getFloat() throws Exception{
        return Math.abs( param1.getFloat() );
    }
    double getDouble() throws Exception{
        return Math.abs( param1.getDouble() );
    }
    long getMoney() throws Exception{
        return Math.abs( param1.getMoney() );
    }
    MutableNumeric getNumeric() throws Exception{
		if(param1.isNull()) return null;
        MutableNumeric num = param1.getNumeric();
        if(num.getSignum() < 0) num.setSignum(1);
        return num;
    }
    Object getObject() throws Exception{
		if(param1.isNull()) return null;
        Object para1 = param1.getObject();
        switch(param1.getDataType()){
        case SQLTokenizer.FLOAT:
        case SQLTokenizer.DOUBLE:
            double dValue = ((Double)para1).doubleValue();
            return (dValue<0) ? new Double(-dValue) : para1;
        case SQLTokenizer.REAL:
            double fValue = ((Float)para1).floatValue();
            return (fValue<0) ? new Float(-fValue) : para1;
        case SQLTokenizer.BIGINT:
            long lValue = ((Number)para1).longValue();
            return (lValue<0) ? new Long(-lValue) : para1;
        case SQLTokenizer.TINYINT:
        case SQLTokenizer.SMALLINT:
        case SQLTokenizer.INT:
            int iValue = ((Number)para1).intValue();
            return (iValue<0) ? new Integer(-iValue) : para1;
        case SQLTokenizer.NUMERIC:
        case SQLTokenizer.DECIMAL:
            MutableNumeric nValue = (MutableNumeric)para1;
            if(nValue.getSignum() <0) nValue.setSignum(1);
            return nValue;
        case SQLTokenizer.MONEY:
            Money mValue = (Money)para1;
            if(mValue.value <0) mValue.value = -mValue.value;
            return mValue;
        default: throw createUnspportedDataType(param1.getDataType());
        }
    }
    String getString() throws Exception{
        Object obj = getObject();
        if(obj == null) return null;
        return obj.toString();
    }
}
package smallsql.database;
abstract class ExpressionFunctionReturnP1 extends ExpressionFunction {
    boolean isNull() throws Exception{
        return param1.isNull();
    }
    Object getObject() throws Exception{
		if(isNull()) return null;
        int dataType = getDataType();
        switch(dataType){
	        case SQLTokenizer.BIT:
	        case SQLTokenizer.BOOLEAN:
	                return getBoolean() ? Boolean.TRUE : Boolean.FALSE;
	        case SQLTokenizer.BINARY:
	        case SQLTokenizer.VARBINARY:
	                return getBytes();
	        case SQLTokenizer.TINYINT:
	        case SQLTokenizer.SMALLINT:
	        case SQLTokenizer.INT:
	                return new Integer( getInt() );
	        case SQLTokenizer.BIGINT:
	                return new Long( getLong() );
	        case SQLTokenizer.REAL:
	                return new Float( getFloat() );
	        case SQLTokenizer.FLOAT:
	        case SQLTokenizer.DOUBLE:
	                return new Double( getDouble() );
	        case SQLTokenizer.MONEY:
	        case SQLTokenizer.SMALLMONEY:
	                return Money.createFromUnscaledValue( getMoney() );
	        case SQLTokenizer.NUMERIC:
	        case SQLTokenizer.DECIMAL:
	                return getNumeric();
	        case SQLTokenizer.CHAR:
	        case SQLTokenizer.NCHAR:
	        case SQLTokenizer.VARCHAR:
	        case SQLTokenizer.NVARCHAR:
	        case SQLTokenizer.LONGNVARCHAR:
	        case SQLTokenizer.LONGVARCHAR:
	        		return getString();
	        case SQLTokenizer.LONGVARBINARY:
	                return getBytes();
			case SQLTokenizer.DATE:
			case SQLTokenizer.TIME:
			case SQLTokenizer.TIMESTAMP:
			case SQLTokenizer.SMALLDATETIME:
				return new DateTime( getLong(), dataType );
	        case SQLTokenizer.UNIQUEIDENTIFIER:
	                return getBytes();
	        default: throw createUnspportedDataType(param1.getDataType());
	    }
    }
	int getDataType() {
		return param1.getDataType();
	}
	int getPrecision() {
		return param1.getPrecision();
	}
	final int getScale(){
		return param1.getScale();
	}
}
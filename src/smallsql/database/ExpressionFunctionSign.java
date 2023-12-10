package smallsql.database;
final class ExpressionFunctionSign extends ExpressionFunctionReturnInt {
	final int getFunction() {
		return SQLTokenizer.SIGN;
	}
	final int getInt() throws Exception {
		if(param1.isNull()) return 0;
		switch(ExpressionArithmetic.getBestNumberDataType(param1.getDataType())){
			case SQLTokenizer.INT:
				int intValue = param1.getInt();
				if(intValue < 0)
					return -1;
				if(intValue > 0)
					return 1;
				return 0;
			case SQLTokenizer.BIGINT:
				long longValue = param1.getLong();
				if(longValue < 0)
					return -1;
				if(longValue > 0)
					return 1;
				return 0;
			case SQLTokenizer.MONEY:
				longValue = param1.getMoney();
				if(longValue < 0)
					return -1;
				if(longValue > 0)
					return 1;
				return 0;
			case SQLTokenizer.DECIMAL:
				return param1.getNumeric().getSignum();
			case SQLTokenizer.DOUBLE:
				double doubleValue = param1.getDouble();
				if(doubleValue < 0)
					return -1;
				if(doubleValue > 0)
					return 1;
				return 0;
			default:
				throw new Error();
		}
	}
}
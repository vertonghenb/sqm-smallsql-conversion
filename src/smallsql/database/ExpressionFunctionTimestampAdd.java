package smallsql.database;
public class ExpressionFunctionTimestampAdd extends ExpressionFunction {
	final private int interval;
	ExpressionFunctionTimestampAdd(int intervalType, Expression p1, Expression p2){
		interval = ExpressionFunctionTimestampDiff.mapIntervalType( intervalType );
		setParams( new Expression[]{p1,p2});
	}
	int getFunction() {
		return SQLTokenizer.TIMESTAMPADD;
	}
	boolean isNull() throws Exception {
		return param1.isNull() || param2.isNull();
	}
	boolean getBoolean() throws Exception {
		return getLong() != 0;
	}
	int getInt() throws Exception {
		return (int)getLong();
	}
	long getLong() throws Exception {
		if(isNull()) return 0;
		switch(interval){
			case SQLTokenizer.SQL_TSI_FRAC_SECOND:
				return param2.getLong() + param1.getLong();
			case SQLTokenizer.SQL_TSI_SECOND:
				return param2.getLong() + param1.getLong() * 1000;
			case SQLTokenizer.SQL_TSI_MINUTE:
				return param2.getLong() + param1.getLong() * 60000;
			case SQLTokenizer.SQL_TSI_HOUR:
				return param2.getLong() + param1.getLong() * 3600000;
			case SQLTokenizer.SQL_TSI_DAY:
				return param2.getLong() + param1.getLong() * 86400000;
			case SQLTokenizer.SQL_TSI_WEEK:{
				return param2.getLong() + param1.getLong() * 604800000;
			}case SQLTokenizer.SQL_TSI_MONTH:{
				DateTime.Details details2 = new DateTime.Details(param2.getLong());
				details2.month += param1.getLong();
				return DateTime.calcMillis(details2);
			}
			case SQLTokenizer.SQL_TSI_QUARTER:{
				DateTime.Details details2 = new DateTime.Details(param2.getLong());
				details2.month += param1.getLong() * 3;
				return DateTime.calcMillis(details2);
			}
			case SQLTokenizer.SQL_TSI_YEAR:{
				DateTime.Details details2 = new DateTime.Details(param2.getLong());
				details2.year += param1.getLong();
				return DateTime.calcMillis(details2);
			}
			default: throw new Error();
		}
	}
	float getFloat() throws Exception {
		return getLong();
	}
	double getDouble() throws Exception {
		return getLong();
	}
	long getMoney() throws Exception {
		return getLong() * 10000;
	}
	MutableNumeric getNumeric() throws Exception {
		if(isNull()) return null;
		return new MutableNumeric(getLong());
	}
	Object getObject() throws Exception {
		if(isNull()) return null;
		return new DateTime( getLong(), SQLTokenizer.TIMESTAMP );
	}
	String getString() throws Exception {
		if(isNull()) return null;
		return new DateTime( getLong(), SQLTokenizer.TIMESTAMP ).toString();
	}
	int getDataType() {
		return SQLTokenizer.TIMESTAMP;
	}
}
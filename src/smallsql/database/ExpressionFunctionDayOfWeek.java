package smallsql.database;
final class ExpressionFunctionDayOfWeek extends ExpressionFunctionReturnInt {
	final int getFunction() {
		return SQLTokenizer.DAYOFWEEK;
	}
	final int getInt() throws Exception {
		if(param1.isNull()) return 0;
		return DateTime.dayOfWeek(param1.getLong())+1;
	}
}
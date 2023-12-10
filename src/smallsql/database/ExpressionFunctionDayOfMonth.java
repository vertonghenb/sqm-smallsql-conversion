package smallsql.database;
final class ExpressionFunctionDayOfMonth extends ExpressionFunctionReturnInt {
	final int getFunction() {
		return SQLTokenizer.DAYOFMONTH;
	}
	final int getInt() throws Exception {
		if(param1.isNull()) return 0;
		DateTime.Details details = new DateTime.Details(param1.getLong());
		return details.day;
	}
}
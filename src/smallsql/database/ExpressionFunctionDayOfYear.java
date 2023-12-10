package smallsql.database;
final class ExpressionFunctionDayOfYear extends ExpressionFunctionReturnInt {
	final int getFunction() {
		return SQLTokenizer.DAYOFYEAR;
	}
	final int getInt() throws Exception {
		if(param1.isNull()) return 0;
		DateTime.Details details = new DateTime.Details(param1.getLong());
		return details.dayofyear+1;
	}
}
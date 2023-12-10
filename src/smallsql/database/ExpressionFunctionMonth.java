package smallsql.database;
final class ExpressionFunctionMonth extends ExpressionFunctionReturnInt {
	final int getFunction() {
		return SQLTokenizer.MONTH;
	}
	final int getInt() throws Exception {
		if(param1.isNull()) return 0;
		DateTime.Details details = new DateTime.Details(param1.getLong());
		return details.month+1;
	}
}
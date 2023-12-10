package smallsql.database;
final class ExpressionFunctionYear extends ExpressionFunctionReturnInt {
	final int getFunction() {
		return SQLTokenizer.YEAR;
	}
	final int getInt() throws Exception {
		if(param1.isNull()) return 0;
		DateTime.Details details = new DateTime.Details(param1.getLong());
		return details.year;
	}
}
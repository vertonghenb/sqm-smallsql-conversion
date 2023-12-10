package smallsql.database;
class ColumnExpression extends Column {
	final private Expression expr;
	ColumnExpression(Expression expr){
		this.expr = expr;
	}
	String getName(){
		return expr.getAlias();
	}
	boolean isAutoIncrement(){
		return expr.isAutoIncrement();
	}
	boolean isCaseSensitive(){
		return expr.isCaseSensitive();
	}
	boolean isNullable(){
		return expr.isNullable();
	}
	int getDataType(){
		return expr.getDataType();
	}
	int getDisplaySize(){
		return expr.getDisplaySize();
	}
	int getScale(){
		return expr.getScale();
	}
	int getPrecision(){
		return expr.getPrecision();
	}
}
package smallsql.database;
import java.sql.*;
import smallsql.database.language.Language;
class GroupResult extends MemoryResult{
	private Expression currentGroup; 
	private RowSource from;
	private Expressions groupBy; 
    private Expressions expressions = new Expressions(); 
	private Expressions internalExpressions = new Expressions(); 
	GroupResult(CommandSelect cmd, RowSource from, Expressions groupBy, Expression having, Expressions orderBy) throws SQLException{
		this.from = from;
		this.groupBy = groupBy;
		if(groupBy != null){
			for(int i=0; i<groupBy.size(); i++){
				Expression left = groupBy.get(i);
				int idx = addInternalExpressionFromGroupBy( left );
				ExpressionName right = new ExpressionName(null);
				right.setFrom(this, idx, new ColumnExpression(left));
				Expression expr = new ExpressionArithmetic( left, right, ExpressionArithmetic.EQUALS_NULL);
				currentGroup = (currentGroup == null) ? 
								expr :
								new ExpressionArithmetic( currentGroup, expr, ExpressionArithmetic.AND );
			}
		}
		expressions = internalExpressions;
        for(int c=0; c<expressions.size(); c++){
            addColumn(new ColumnExpression(expressions.get(c)));
        }
		patchExpressions( cmd.columnExpressions );
		if(having != null) having = patchExpression( having );
		patchExpressions( orderBy );
	}
	final private int addInternalExpressionFromGroupBy(Expression expr) throws SQLException{
		int type = expr.getType();
		if(type >= Expression.GROUP_BEGIN){
				throw SmallSQLException.create(Language.GROUP_AGGR_INVALID, expr);
		}else{
			int idx = internalExpressions.indexOf(expr);
			if(idx >= 0) return idx;
			internalExpressions.add(expr);
			return internalExpressions.size()-1;
		}
	}
	final private int addInternalExpressionFromSelect(Expression expr) throws SQLException{
		int type = expr.getType();
		if(type == Expression.NAME){
			int idx = internalExpressions.indexOf(expr);
			if(idx >= 0) return idx;
			throw SmallSQLException.create(Language.GROUP_AGGR_NOTPART, expr);
		}else
		if(type >= Expression.GROUP_BEGIN){
			int idx = internalExpressions.indexOf(expr);
			if(idx >= 0) return idx;
			internalExpressions.add(expr);
			return internalExpressions.size()-1;
		}else{
			int idx = internalExpressions.indexOf(expr);
			if(idx >= 0) return idx;
			Expression[] params = expr.getParams();
			if(params != null){
				for(int p=0; p<params.length; p++){
					addInternalExpressionFromSelect( params[p]);
				}
			}
			return -1;
		}
	}
	final private void patchExpressions(Expressions exprs) throws SQLException{
		if(exprs == null) return;
		for(int i=0; i<exprs.size(); i++){
			exprs.set(i, patchExpression(exprs.get(i)));
		}	
	}
	final private void patchExpressions(Expression expression) throws SQLException{
		Expression[] params = expression.getParams();
		if(params == null) return;
		for(int i=0; i<params.length; i++){
			expression.setParamAt( patchExpression(params[i]), i);
		}
	}
	final private Expression patchExpression(Expression expr) throws SQLException{
		int idx = addInternalExpressionFromSelect( expr );
		if(idx>=0){
            Expression origExpression = expr;
			ExpressionName exprName;
			if(expr instanceof ExpressionName){
				exprName = (ExpressionName)expr;
			}else{
				expr = exprName = new ExpressionName(expr.getAlias());
			}
			Column column = exprName.getColumn();
			if(column == null){
				column = new Column();
                exprName.setFrom(this, idx, column);
				switch(exprName.getType()){
					case Expression.MAX:
					case Expression.MIN:
					case Expression.FIRST:
					case Expression.LAST:
					case Expression.SUM:
						Expression baseExpression = exprName.getParams()[0];
						column.setPrecision(baseExpression.getPrecision());
						column.setScale(baseExpression.getScale());
						break;
                    default:
                        column.setPrecision(origExpression.getPrecision());
                        column.setScale(origExpression.getScale());
				}
				column.setDataType(exprName.getDataType());
			}else{
				exprName.setFrom(this, idx, column);
			}
		}else{
			patchExpressions(expr);
		}
		return expr;
	}
	final void execute() throws Exception{
        super.execute();
		from.execute();
		NextRow:
		while(from.next()){
			beforeFirst();
			while(next()){
				if(currentGroup == null || currentGroup.getBoolean()){
					accumulateRow();
					continue NextRow;
				}
			}
			addGroupRow();
			accumulateRow();
		}
		if(getRowCount() == 0 && groupBy == null){
			addGroupRow();
		}
		beforeFirst();
	}
	final private void addGroupRow(){
		ExpressionValue[] newRow = currentRow = new ExpressionValue[ expressions.size()];
		for(int i=0; i<newRow.length; i++){
			Expression expr = expressions.get(i);
			int type = expr.getType();
			if(type < Expression.GROUP_BEGIN) type = Expression.GROUP_BY; 
			newRow[i] = new ExpressionValue( type );
		}
		addRow(newRow);
	}
	final private void accumulateRow() throws Exception{
		for(int i=0; i<currentRow.length; i++){
			Expression src = expressions.get(i);
			currentRow[i].accumulate(src);
		}
	}
}
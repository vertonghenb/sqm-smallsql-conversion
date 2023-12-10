package smallsql.database;
import java.sql.*;
import smallsql.database.language.Language;
class CommandSelect extends Command{
    private DataSources tables; 
	private Expression where;
    RowSource from;
    private Expressions groupBy;
    private Expression having;
    private Expressions orderBy;
    private boolean isAggregateFunction;
    private int maxRows = -1;
    private boolean isDistinct; 
    CommandSelect(Logger log){
		super(log);
    }
	CommandSelect(Logger log, Expressions columnExpressions){
		super(log, columnExpressions);
	}
    boolean compile(SSConnection con) throws Exception{
        boolean needCompile = false;
        if(tables != null){
            for(int i=0; i<tables.size(); i++){
				DataSource fromEntry = tables.get(i);
                needCompile |= fromEntry.init( con );
            }
        }
		if(from == null){
			from = new NoFromResult();
			tables = new DataSources();
			needCompile = true;
		}
        if(!needCompile) return false;
        for(int i=0; i<columnExpressions.size(); i++){
            Expression col = columnExpressions.get(i);
            if(col.getAlias() == null){
                col.setAlias("col" + (i+1));
            }
            if(col.getType() != Expression.NAME){
                compileLinkExpressionParams(col);
                continue;
            }
            ExpressionName expr = (ExpressionName)col;
            if("*".equals( expr.getName() )){
                String tableAlias = expr.getTableAlias();
                if(tableAlias != null){
                    int t=0;
                    for(; t<tables.size(); t++){
						DataSource fromEntry = tables.get(t);
                        if(tableAlias.equalsIgnoreCase( fromEntry.getAlias() )){
                            TableView table = fromEntry.getTableView();
                            columnExpressions.remove(i);
                            i = compileAdd_All_Table_Columns( fromEntry, table, i ) - 1;
                            break;
                        }
                    }
                    if(t==tables.size()) throw SmallSQLException.create(Language.COL_WRONG_PREFIX, new Object[] {tableAlias});
                }else{
                    columnExpressions.remove(i);
                    for(int t=0; t<tables.size(); t++){
						DataSource fromEntry = tables.get(t);
                        TableView table = fromEntry.getTableView();
                        i = compileAdd_All_Table_Columns( fromEntry, table, i );
                    }
                    i--;
                }
            }else{
                compileLinkExpressionName( expr );
            }
        }
        if(where != null) compileLinkExpression( where );
        if(having != null) compileLinkExpression( having );
        if(orderBy != null) {
            for(int i=0; i<orderBy.size(); i++){
            	compileLinkExpression( orderBy.get(i));
            }
        }
		if(groupBy != null){
			for(int i=0; i<groupBy.size(); i++){
				compileLinkExpression( groupBy.get(i) );
			}
		}
        if(from instanceof Join){
            compileJoin( (Join)from );
        }
        if(where != null){
        	from = new Where( from, where );
        }
		if(isGroupResult()) {
			from = new GroupResult( this, from, groupBy, having, orderBy);
			if(having != null){
                from = new Where( from, having );
            }
		}
		if(isDistinct){
			from = new Distinct( from, columnExpressions );
		}
		if(orderBy != null){
			from = new SortedResult( from, orderBy );
		}
		return true;
    }
    final boolean isGroupResult(){
    	return groupBy != null || having != null || isAggregateFunction;
    }
    private void compileJoin( Join singleJoin ) throws Exception{
        if(singleJoin.condition != null) compileLinkExpressionParams( singleJoin.condition );
        if(singleJoin.left instanceof Join){
            compileJoin( (Join)singleJoin.left );
        }
        if(singleJoin.right instanceof Join){
            compileJoin( (Join)singleJoin.right );
        }
    }
    private void compileLinkExpression( Expression expr) throws Exception{
		if(expr.getType() == Expression.NAME)
			 compileLinkExpressionName( (ExpressionName)expr);
		else compileLinkExpressionParams( expr );
    }
    private void compileLinkExpressionName(ExpressionName expr) throws Exception{
        String tableAlias = expr.getTableAlias();
        if(tableAlias != null){
            int t = 0;
            for(; t < tables.size(); t++){
                DataSource fromEntry = tables.get(t);
                if(tableAlias.equalsIgnoreCase(fromEntry.getAlias())){
                    TableView table = fromEntry.getTableView();
                    int colIdx = table.findColumnIdx(expr.getName());
                    if(colIdx >= 0){
                        expr.setFrom(fromEntry, colIdx, table);
                        break;
                    }else
                        throw SmallSQLException.create(Language.COL_INVALID_NAME, new Object[]{expr.getName()});
                }
            }
            if(t == tables.size())
                throw SmallSQLException.create(Language.COL_WRONG_PREFIX, tableAlias);
        }else{
            boolean isSetFrom = false;
            for(int t = 0; t < tables.size(); t++){
                DataSource fromEntry = tables.get(t);
                TableView table = fromEntry.getTableView();
                int colIdx = table.findColumnIdx(expr.getName());
                if(colIdx >= 0){
                    if(isSetFrom){
                        throw SmallSQLException.create(Language.COL_AMBIGUOUS, expr.getName());
                    }
                    isSetFrom = true;
                    expr.setFrom(fromEntry, colIdx, table);
                }
            }
            if(!isSetFrom){
                throw SmallSQLException.create(Language.COL_INVALID_NAME, expr.getName());
            }
        }
        compileLinkExpressionParams(expr);
    }
    private void compileLinkExpressionParams(Expression expr) throws Exception{
        Expression[] expParams = expr.getParams();
		isAggregateFunction = isAggregateFunction || expr.getType() >= Expression.GROUP_BEGIN;
        if(expParams != null){
            for(int k=0; k<expParams.length; k++){
                Expression param = expParams[k];
				int paramType = param.getType();
				isAggregateFunction = isAggregateFunction || paramType >= Expression.GROUP_BEGIN;
                if(paramType == Expression.NAME)
                     compileLinkExpressionName( (ExpressionName)param );
                else compileLinkExpressionParams( param );
            }
        }
        expr.optimize();
    }
    private final int compileAdd_All_Table_Columns( DataSource fromEntry, TableView table, int position){
        for(int k=0; k<table.columns.size(); k++){
            ExpressionName expr = new ExpressionName( table.columns.get(k).getName() );
            expr.setFrom( fromEntry, k, table );
            columnExpressions.add( position++, expr );
        }
        return position;
    }
    void executeImpl(SSConnection con, SSStatement st) throws Exception{
        compile(con);
        if((st.rsType == ResultSet.TYPE_SCROLL_INSENSITIVE || st.rsType == ResultSet.TYPE_SCROLL_SENSITIVE) &&
        	!from.isScrollable()){
        	from = new Scrollable(from);
        }
        from.execute();
        rs =  new SSResultSet( st, this );
    }
    void beforeFirst() throws Exception{
		from.beforeFirst();
    }
	boolean isBeforeFirst() throws SQLException{
		return from.isBeforeFirst();
	}
	boolean isFirst() throws SQLException{
		return from.isFirst();
	}
    boolean first() throws Exception{
		return from.first();
    }
	boolean previous() throws Exception{
		return from.previous();
	}
    boolean next() throws Exception{
        if(maxRows >= 0 && from.getRow() >= maxRows){
        	from.afterLast();
        	return false;
        }
		return from.next();
    }
	final boolean last() throws Exception{
		if(maxRows >= 0){
            if(maxRows == 0){
                from.beforeFirst();
                return false;
            }
			return from.absolute(maxRows);
		}
		return from.last();
	}
	final void afterLast() throws Exception{
		from.afterLast();
	}
	boolean isLast() throws Exception{
		return from.isLast();
	}
	boolean isAfterLast() throws Exception{
		return from.isAfterLast();
	}
	final boolean absolute(int row) throws Exception{
		return from.absolute(row);
	}
	final boolean relative(int rows) throws Exception{
		return from.relative(rows);
	}
	final int getRow() throws Exception{
		int row = from.getRow();
		if(maxRows >= 0 && row > maxRows) return 0;
		return row;
	}
	final void updateRow(SSConnection con, Expression[] newRowSources) throws SQLException{
		int savepoint = con.getSavepoint();
		try{
			for(int t=0; t<tables.size(); t++){
				TableViewResult result = TableViewResult.getTableViewResult( tables.get(t) );
				TableView table = result.getTableView();
				Columns tableColumns = table.columns;
				int count = tableColumns.size();
				Expression[] updateValues = new Expression[count];
				boolean isUpdateNeeded = false;
				for(int i=0; i<columnExpressions.size(); i++){
					Expression src = newRowSources[i];
					if(src != null && (!(src instanceof ExpressionValue) || !((ExpressionValue)src).isEmpty())){	
						Expression col = columnExpressions.get(i);
						if(!col.isDefinitelyWritable())
							throw SmallSQLException.create(Language.COL_READONLY, new Integer(i));
						ExpressionName exp = (ExpressionName)col;
						if(table == exp.getTable()){
							updateValues[exp.getColumnIndex()] = src;
							isUpdateNeeded = true;
							continue;
						}
					}
				}
				if(isUpdateNeeded){
					result.updateRow(updateValues);
				}
			}
		}catch(Throwable e){
			con.rollback(savepoint);
			throw SmallSQLException.createFromException(e);
		}finally{
			if(con.getAutoCommit()) con.commit();
		}
	}
	final void insertRow(SSConnection con, Expression[] newRowSources) throws SQLException{
		if(tables.size() > 1)
			throw SmallSQLException.create(Language.JOIN_INSERT);
		if(tables.size() == 0)
			throw SmallSQLException.create(Language.INSERT_WO_FROM);
		int savepoint = con.getSavepoint();
		try{
			TableViewResult result = TableViewResult.getTableViewResult( tables.get(0) );
			TableView table = result.getTableView();
			Columns tabColumns = table.columns;
			int count = tabColumns.size();
			Expression[] updateValues = new Expression[count];
			if(newRowSources != null){
				for(int i=0; i<columnExpressions.size(); i++){
					Expression src = newRowSources[i];
					if(src != null && (!(src instanceof ExpressionValue) || !((ExpressionValue)src).isEmpty())){	
						Expression rsColumn = columnExpressions.get(i); 
						if(!rsColumn.isDefinitelyWritable())
							throw SmallSQLException.create(Language.COL_READONLY, new Integer(i));
						ExpressionName exp = (ExpressionName)rsColumn;
						if(table == exp.getTable()){
							updateValues[exp.getColumnIndex()] = src;
							continue;
						}
					}
					updateValues[i] = null;
				}
			}
			result.insertRow(updateValues);
		}catch(Throwable e){
			con.rollback(savepoint);
			throw SmallSQLException.createFromException(e);
		}finally{
			if(con.getAutoCommit()) con.commit();
		}
	}
	final void deleteRow(SSConnection con) throws SQLException{
		int savepoint = con.getSavepoint();
		try{
			if(tables.size() > 1)
				throw SmallSQLException.create(Language.JOIN_DELETE);
			if(tables.size() == 0)
				throw SmallSQLException.create(Language.DELETE_WO_FROM);
			TableViewResult.getTableViewResult( tables.get(0) ).deleteRow();
		}catch(Throwable e){
			con.rollback(savepoint);
			throw SmallSQLException.createFromException(e);
		}finally{
			if(con.getAutoCommit()) con.commit();
		}
	}
	public int findColumn(String columnName) throws SQLException {
		Expressions columns = columnExpressions;
		for(int i=0; i<columns.size(); i++){
			if(columnName.equalsIgnoreCase(columns.get(i).getAlias()))
				return i;
		}
		throw SmallSQLException.create(Language.COL_MISSING, columnName);
	}
	final void setDistinct(boolean distinct){
		this.isDistinct = distinct;
	}
    final void setSource(RowSource join){
        this.from = join;
    }
    final void setTables( DataSources from ){
        this.tables = from;
    }
	final void setWhere( Expression where ){
		this.where = where;
	}
	final void setGroup(Expressions group){
        this.groupBy = group;
    }
	final void setHaving(Expression having){
        this.having = having;
    }
	final void setOrder(Expressions order){
        this.orderBy = order;
    }
	final void setMaxRows(int max){
		maxRows = max;
	}
    final int getMaxRows(){
        return maxRows;
    }
}
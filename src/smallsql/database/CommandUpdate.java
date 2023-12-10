package smallsql.database;
class CommandUpdate extends CommandSelect {
	private Expressions sources = new Expressions();
	private Expression[] newRowSources;
	CommandUpdate( Logger log ){
		super(log);
	}
	void addSetting(Expression dest, Expression source){
		columnExpressions.add(dest);
		sources.add(source);
	}
	void executeImpl(SSConnection con, SSStatement st) throws Exception {
		int count = columnExpressions.size();
		columnExpressions.addAll(sources);
		compile(con);
		columnExpressions.setSize(count);
		newRowSources = sources.toArray();
		updateCount = 0;
		from.execute();
		for(int i=0; i<columnExpressions.size(); i++){
		    ExpressionName expr = (ExpressionName)columnExpressions.get(i);
		    DataSource ds = expr.getDataSource();
		    TableResult tableResult = (TableResult)ds;
		    tableResult.lock = SQLTokenizer.UPDATE;
		}
		while(true){
            synchronized(con.getMonitor()){
                if(!next()){
                    return;
                }
                updateRow(con, newRowSources);
            }
			updateCount++;
		}
	}
}
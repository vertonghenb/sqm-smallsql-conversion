package smallsql.database;
class CommandDelete extends CommandSelect {
	CommandDelete(Logger log){
		super(log);
	}
	void executeImpl(SSConnection con, SSStatement st) throws Exception {
		compile(con);
		TableViewResult result = TableViewResult.getTableViewResult(from);
		updateCount = 0;
		from.execute();
		while(next()){
			result.deleteRow();
			updateCount++;
		}
	}
}
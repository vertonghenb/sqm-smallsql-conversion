package smallsql.database;
import smallsql.database.language.Language;
class ExpressionInSelect extends ExpressionArithmetic {
	final private CommandSelect cmdSel;
	final private Index index = new Index(true);
	final private SSConnection con;
	ExpressionInSelect(SSConnection con, Expression left, CommandSelect cmdSel, int operation) {
		super(left, (Expressions)null, operation);
		this.cmdSel = cmdSel;
		this.con = con;
	}
	private void loadInList() throws Exception{
		if(cmdSel.compile(con)){
			cmdSel.from.execute();
			if(cmdSel.columnExpressions.size() != 1)
				throw SmallSQLException.create(Language.SUBQUERY_COL_COUNT, new Integer(cmdSel.columnExpressions.size()));
			index.clear();
			while(cmdSel.next()){
				try{
					index.addValues(0, cmdSel.columnExpressions );
				}catch(Exception e){
				}
			}
		}
	}
	boolean isInList() throws Exception{
		loadInList();
		return index.findRows(getParams(), false, null) != null;
	}
}
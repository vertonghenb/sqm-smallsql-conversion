package smallsql.database;
import java.sql.SQLException;
import smallsql.database.language.Language;
abstract class TableViewResult extends DataSource {
	SSConnection con;
	private String alias;
	private long tableTimestamp;
	int lock = SQLTokenizer.SELECT;
	static TableViewResult createResult(TableView tableView){
		if(tableView instanceof Table)
			return new TableResult((Table)tableView);
		return new ViewResult( (View)tableView );
	}
	static TableViewResult getTableViewResult(RowSource from) throws SQLException{
		if(from instanceof Where){
			from = ((Where)from).getFrom();
		}
		if(from instanceof TableViewResult){
			return (TableViewResult)from;
		}
		throw SmallSQLException.create(Language.ROWSOURCE_READONLY);
	}
	void setAlias( String alias ){
		this.alias = alias;
	}
	String getAlias(){
		return (alias != null) ? alias : getTableView().name;
	}
	boolean hasAlias(){
		return alias != null;
	}
	boolean init( SSConnection con ) throws Exception{
		TableView tableView = getTableView();
		if(tableTimestamp != tableView.getTimestamp()){
			this.con = con;
			tableTimestamp = tableView.getTimestamp();
			return true;
		}
		return false;
	}
	abstract void deleteRow() throws SQLException;
	abstract void updateRow(Expression[] updateValues) throws Exception;
	abstract void insertRow(Expression[] updateValues) throws Exception;
	final boolean isScrollable(){
		return false;
	}
}
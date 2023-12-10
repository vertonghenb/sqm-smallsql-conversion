package smallsql.database;
import java.sql.*;
class ViewResult extends TableViewResult {
	final private View view;
	final private Expressions columnExpressions;
	final private CommandSelect commandSelect;
	ViewResult(View view){
		this.view = view;
		this.columnExpressions = view.commandSelect.columnExpressions;
		this.commandSelect     = view.commandSelect;
	}
	ViewResult(SSConnection con, CommandSelect commandSelect) throws SQLException{
		try{
			this.view = new View( con, commandSelect);
			this.columnExpressions = commandSelect.columnExpressions;
			this.commandSelect     = commandSelect;
		}catch(Exception e){
			throw SmallSQLException.createFromException(e);
		}
	}
	boolean init( SSConnection con ) throws Exception{
		if(super.init(con)){
			commandSelect.compile(con);
			return true;
		}
		return false;
	}
	TableView getTableView(){
		return view;
	}
	void deleteRow() throws SQLException{
		commandSelect.deleteRow(con);
	}
	void updateRow(Expression[] updateValues) throws Exception{
		commandSelect.updateRow(con, updateValues);
	}
	void insertRow(Expression[] updateValues) throws Exception{
		commandSelect.insertRow(con, updateValues);
	}
	boolean isNull(int colIdx) throws Exception {
		return columnExpressions.get(colIdx).isNull();
	}
	boolean getBoolean(int colIdx) throws Exception {
		return columnExpressions.get(colIdx).getBoolean();
	}
	int getInt(int colIdx) throws Exception {
		return columnExpressions.get(colIdx).getInt();
	}
	long getLong(int colIdx) throws Exception {
		return columnExpressions.get(colIdx).getLong();
	}
	float getFloat(int colIdx) throws Exception {
		return columnExpressions.get(colIdx).getFloat();
	}
	double getDouble(int colIdx) throws Exception {
		return columnExpressions.get(colIdx).getDouble();
	}
	long getMoney(int colIdx) throws Exception {
		return columnExpressions.get(colIdx).getMoney();
	}
	MutableNumeric getNumeric(int colIdx) throws Exception {
		return columnExpressions.get(colIdx).getNumeric();
	}
	Object getObject(int colIdx) throws Exception {
		return columnExpressions.get(colIdx).getObject();
	}
	String getString(int colIdx) throws Exception {
		return columnExpressions.get(colIdx).getString();
	}
	byte[] getBytes(int colIdx) throws Exception {
		return columnExpressions.get(colIdx).getBytes();
	}
	int getDataType(int colIdx) {
		return columnExpressions.get(colIdx).getDataType();
	}
	void beforeFirst() throws Exception {
		commandSelect.beforeFirst();
	}
	boolean isBeforeFirst() throws SQLException{
		return commandSelect.isBeforeFirst();
	}
	boolean isFirst() throws SQLException{
		return commandSelect.isFirst();
	}
	boolean first() throws Exception {
		return commandSelect.first();
	}
	boolean previous() throws Exception{
		return commandSelect.previous();
	}
	boolean next() throws Exception {
		return commandSelect.next();
	}
	boolean last() throws Exception{
		return commandSelect.last();
	}
	boolean isLast() throws Exception{
		return commandSelect.isLast();
	}
	boolean isAfterLast() throws Exception{
		return commandSelect.isAfterLast();
	}
	void afterLast() throws Exception{
		commandSelect.afterLast();
	}
	boolean absolute(int row) throws Exception{
		return commandSelect.absolute(row);
	}
	boolean relative(int rows) throws Exception{
		return commandSelect.relative(rows);
	}
	int getRow() throws Exception{
		return commandSelect.getRow();
	}
	long getRowPosition() {
		return commandSelect.from.getRowPosition();
	}
	void setRowPosition(long rowPosition) throws Exception {
		commandSelect.from.setRowPosition(rowPosition);
	}
	final boolean rowInserted(){
		return commandSelect.from.rowInserted();
	}
	final boolean rowDeleted(){
		return commandSelect.from.rowDeleted();
	}
	void nullRow() {
		commandSelect.from.nullRow();
	}
	void noRow() {
		commandSelect.from.noRow();
	}
	final void execute() throws Exception{
		commandSelect.from.execute();
	}
}
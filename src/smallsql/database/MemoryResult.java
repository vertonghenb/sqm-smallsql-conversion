package smallsql.database;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import smallsql.database.language.Language;
class MemoryResult extends DataSource {
	ExpressionValue[] currentRow;
    private final Columns columns = new Columns();
	private int rowIdx = -1;
	private List rowList = new ArrayList(); 
	MemoryResult(){}
	MemoryResult(Object[][] data, int colCount) throws SQLException{
        for(int c=0; c<colCount; c++){
            Column column = new Column();
            column.setDataType(SQLTokenizer.NULL);
            columns.add( column );
        }
		for(int r=0; r<data.length; r++){
			Object[] row = data[r];
            ExpressionValue[] rowValues = new ExpressionValue[row.length];
			addRow(rowValues);
			for(int c=0; c<colCount; c++){
                ExpressionValue expr = rowValues[c] = new ExpressionValue();
				expr.set( row[c], -1);
                Column column = columns.get(c);
                if(expr.getDataType() != SQLTokenizer.NULL){
                    column.setDataType(expr.getDataType());
                }
                if(expr.getPrecision() > column.getPrecision()){
                    column.setPrecision(expr.getPrecision());
                }
			}
		}
	}
	final void addRow(ExpressionValue[] row){
		rowList.add(row);
	}
    final Column getColumn(int colIdx){
        return columns.get(colIdx);
    }
    final void addColumn(Column column){
        columns.add(column);
    }
	final boolean isScrollable(){
		return true;
	}
	final void beforeFirst(){
		rowIdx = -1;
		currentRow = null;
	}
	final boolean isBeforeFirst(){
		return rowIdx < 0 || rowList.size() == 0;
	}
	final boolean isFirst(){
		return rowIdx == 0 && currentRow != null;
	}
	final boolean first(){
		rowIdx = 0;
		return move();
	}
	final boolean previous(){
		if(rowIdx-- < 0) rowIdx = -1;
		return move();
	}
	final boolean next(){
		rowIdx++;
		return move();
	}
	final boolean last(){
		rowIdx = rowList.size() - 1;
		return move();
	}
	final boolean isLast(){
		return rowIdx == rowList.size() - 1 && currentRow != null;
	}
	final boolean isAfterLast(){
		return rowIdx >= rowList.size() || rowList.size() == 0;
	}
	final void afterLast(){
		rowIdx = rowList.size();
		currentRow = null;
	}
	final boolean absolute(int row) throws SQLException{
		if(row == 0) throw SmallSQLException.create(Language.ROW_0_ABSOLUTE);
		rowIdx = (row > 0) ?
			Math.min( row - 1, rowList.size() ):
			Math.max( row +rowList.size(), -1 );
		return move();
	}
	final boolean relative(int rows){
		if(rows == 0) return (currentRow != null);
		rowIdx = Math.min( Math.max( rowIdx + rows, -1), rowList.size());
		return move();
	}
	final int getRow(){
		return currentRow == null ? 0 : rowIdx+1;
	}
	final long getRowPosition(){
		return rowIdx;
	}
	final void setRowPosition(long rowPosition) throws Exception{
		rowIdx = (int)rowPosition;
		move();
	}
	final boolean rowInserted(){
		return false;
	}
	final boolean rowDeleted(){
		return false;
	}
	void nullRow(){
		throw new Error();
	}
	void noRow(){
		currentRow = null;
	}
	final private boolean move(){
		if(rowIdx < rowList.size() && rowIdx >= 0){
			currentRow = (ExpressionValue[])rowList.get(rowIdx);
			return true;
		}
		currentRow = null;
		return false;
	}
	boolean isNull( int colIdx ) throws Exception{
		return get( colIdx ).isNull();
	}
	boolean getBoolean( int colIdx ) throws Exception{
		return get( colIdx ).getBoolean();
	}
	int getInt( int colIdx ) throws Exception{
		return get( colIdx ).getInt();
	}
	long getLong( int colIdx ) throws Exception{
		return get( colIdx ).getLong();
	}
	float getFloat( int colIdx ) throws Exception{
		return get( colIdx ).getFloat();
	}
	double getDouble( int colIdx ) throws Exception{
		return get( colIdx ).getDouble();
	}
	long getMoney( int colIdx ) throws Exception{
		return get( colIdx ).getMoney();
	}
	MutableNumeric getNumeric( int colIdx ) throws Exception{
		return get( colIdx ).getNumeric();
	}
	Object getObject( int colIdx ) throws Exception{
		return get( colIdx ).getObject();
	}
	String getString( int colIdx ) throws Exception{
		return get( colIdx ).getString();
	}
	byte[] getBytes( int colIdx ) throws Exception{
		return get( colIdx ).getBytes();
	}
	int getDataType( int colIdx ){
		return columns.get( colIdx ).getDataType();
	}
	final TableView getTableView(){
		return null;
	}
	final void deleteRow() throws Exception{
		throw SmallSQLException.create(Language.RSET_READONLY);
	}
	final void updateRow(Expression[] updateValues) throws Exception{
		throw SmallSQLException.create(Language.RSET_READONLY);
	}
	final void insertRow(Expression[] updateValues) throws Exception{
		throw SmallSQLException.create(Language.RSET_READONLY);
	}
	private Expression get(int colIdx) throws Exception{
		if(currentRow == null) throw SmallSQLException.create(Language.ROW_NOCURRENT);
		return currentRow[ colIdx ];
	}
	int getRowCount(){
		return rowList.size();
	}
	void execute() throws Exception{
        rowList.clear();
	}
}
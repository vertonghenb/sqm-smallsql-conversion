package smallsql.database;
class Where extends RowSource {
	final private RowSource rowSource;
	final private Expression where;
	private int row = 0;
	private boolean isCurrentRow;
	Where(RowSource rowSource, Expression where){
		this.rowSource = rowSource;
		this.where = where;
	}
	RowSource getFrom(){
		return rowSource;
	}
	final private boolean isValidRow() throws Exception{
		return where == null || rowSource.rowInserted() || where.getBoolean();
	}
	final boolean isScrollable() {
		return rowSource.isScrollable();
	}
	final boolean isBeforeFirst(){
		return row == 0;
	}
	final boolean isFirst(){
		return row == 1 && isCurrentRow;
	}
	final boolean isLast() throws Exception{
		if(!isCurrentRow) return false;
		long rowPos = rowSource.getRowPosition();
		boolean isNext = next();
		rowSource.setRowPosition(rowPos);
		return !isNext;
	}
	final boolean isAfterLast(){
		return row > 0 && !isCurrentRow;
	}
	final void beforeFirst() throws Exception {
		rowSource.beforeFirst();
		row = 0;
	}
	final boolean first() throws Exception {
		isCurrentRow = rowSource.first();
		while(isCurrentRow && !isValidRow()){
			isCurrentRow = rowSource.next();
		}
		row = 1;
		return isCurrentRow;
	}
	final boolean previous() throws Exception {
        boolean oldIsCurrentRow = isCurrentRow;
		do{
			isCurrentRow = rowSource.previous();
		}while(isCurrentRow && !isValidRow());
		if(oldIsCurrentRow || isCurrentRow) row--;
		return isCurrentRow;
	}
	final boolean next() throws Exception {
        boolean oldIsCurrentRow = isCurrentRow;
		do{
			isCurrentRow = rowSource.next();
		}while(isCurrentRow && !isValidRow());
		if(oldIsCurrentRow || isCurrentRow) row++;
		return isCurrentRow;
	}
	final boolean last() throws Exception{
		while(next()){}
		return previous();
	}
	final void afterLast() throws Exception {
		while(next()){}
	}
	final int getRow() throws Exception {
		return isCurrentRow ? row : 0;
	}
	final long getRowPosition() {
		return rowSource.getRowPosition();
	}
	final void setRowPosition(long rowPosition) throws Exception {
		rowSource.setRowPosition(rowPosition);
	}
	final void nullRow() {
		rowSource.nullRow();
		row = 0;
	}
	final void noRow() {
		rowSource.noRow();
		row = 0;
	}
	final boolean rowInserted() {
		return rowSource.rowInserted();
	}
	final boolean rowDeleted() {
		return rowSource.rowDeleted();
	}
	final void execute() throws Exception{
		rowSource.execute();
	}
    boolean isExpressionsFromThisRowSource(Expressions columns){
        return rowSource.isExpressionsFromThisRowSource(columns);
    }
}
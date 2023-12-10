package smallsql.database;
final  class NoFromResult extends RowSource {
	private int rowPos; 
	final boolean isScrollable(){
		return true;
	}
	final void beforeFirst(){
		rowPos = 0;
	}
	final boolean isBeforeFirst(){
		return rowPos <= 0;
	}
	final boolean isFirst(){
		return rowPos == 1;
	}
	final boolean first(){
		rowPos = 1;
		return true;
	}
	final boolean previous(){
		rowPos--;
		return rowPos == 1;
	}
	final boolean next(){
		rowPos++;
		return rowPos == 1;
	}
	final boolean last(){
		rowPos = 1;
		return true;
	}
	final boolean isLast(){
		return rowPos == 1;
	}
	final boolean isAfterLast(){
		return rowPos > 1;
	}
	final void afterLast(){
		rowPos = 2;
	}
	final boolean absolute(int row){
		rowPos = (row > 0) ?
			Math.min( row, 1 ) :
			Math.min( row +1, -1 );
		return rowPos == 1;
	}
	final boolean relative(int rows){
		if(rows == 0) return rowPos == 1;
		rowPos = Math.min( Math.max( rowPos + rows, -1), 1);
		return rowPos == 1;
	}
	final int getRow(){
		return rowPos == 1 ? 1 : 0;
	}
	final long getRowPosition() {
		return rowPos;
	}
	final void setRowPosition(long rowPosition){
		rowPos = (int)rowPosition;
	}
	final boolean rowInserted(){
		return false;
	}
	final boolean rowDeleted(){
		return false;
	}
	final void nullRow() {
		throw new Error();
	}
	final void noRow() {
		throw new Error();
	}
	final void execute() throws Exception{}
    boolean isExpressionsFromThisRowSource(Expressions columns){
        return columns.size() == 0;
    }
}
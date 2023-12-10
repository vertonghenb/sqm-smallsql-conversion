package smallsql.database;
final class Distinct extends RowSource {
	final private Expressions distinctColumns;
	final private RowSource rowSource;
	private Index index;
	private int row;
	Distinct(RowSource rowSource, Expressions columns){
		this.rowSource = rowSource;
		this.distinctColumns = columns;
	}
	final void execute() throws Exception{
		rowSource.execute();
		index = new Index(true);	
	}
	final boolean isScrollable() {
		return false;
	}
	final void beforeFirst() throws Exception {
		rowSource.beforeFirst();
		row = 0;
	}
	final boolean first() throws Exception {
		beforeFirst();
		return next();
	}
	final boolean next() throws Exception {
		while(true){
			boolean isNext = rowSource.next();
			if(!isNext) return false;
			Long oldRowOffset = (Long)index.findRows(distinctColumns, true, null);
			long newRowOffset = rowSource.getRowPosition();
			if(oldRowOffset == null){
				index.addValues( newRowOffset, distinctColumns);
				row++;
				return true;
			}else
			if(oldRowOffset.longValue() == newRowOffset){
				row++;
				return true;
			}
		}
	}
	final void afterLast() throws Exception {
		rowSource.afterLast();
		row = 0;
	}
	final int getRow() throws Exception {
		return row;
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
	final boolean rowInserted(){
		return rowSource.rowInserted();
	}
	final boolean rowDeleted() {
		return rowSource.rowDeleted();
	}
    boolean isExpressionsFromThisRowSource(Expressions columns){
        return rowSource.isExpressionsFromThisRowSource(columns);
    }
}
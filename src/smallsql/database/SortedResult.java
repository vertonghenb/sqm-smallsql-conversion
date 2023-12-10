package smallsql.database;
import smallsql.database.language.Language;
final class SortedResult extends RowSource {
	final private Expressions orderBy;
	final private RowSource rowSource;
	private IndexScrollStatus scrollStatus;
	private int row;
    private final LongList insertedRows = new LongList();
	private boolean useSetRowPosition;
    private int sortedRowCount;
    private long lastRowOffset;
	SortedResult(RowSource rowSource, Expressions orderBy){
		this.rowSource = rowSource;
		this.orderBy = orderBy;
	}
	final boolean isScrollable(){
		return true;
	}
	final void execute() throws Exception{
		rowSource.execute();
		Index index = new Index(false);	
        lastRowOffset = -1;
		while(rowSource.next()){
            lastRowOffset = rowSource.getRowPosition();
			index.addValues( lastRowOffset, orderBy);
            sortedRowCount++;
		}
		scrollStatus = index.createScrollStatus(orderBy);
		useSetRowPosition = false;
	}
    final boolean isBeforeFirst(){
        return row == 0;
    }
    final boolean isFirst(){
        return row == 1;
    }
    void beforeFirst() throws Exception {
		scrollStatus.reset();
		row = 0;
		useSetRowPosition = false;
	}
	boolean first() throws Exception {
		beforeFirst();
		return next();
	}
    boolean previous() throws Exception{
        if(useSetRowPosition) throw SmallSQLException.create(Language.ORDERBY_INTERNAL);
        if(currentInsertedRow() == 0){
            scrollStatus.afterLast();
        }
        row--;
        if(currentInsertedRow() >= 0){
            rowSource.setRowPosition( insertedRows.get( currentInsertedRow() ) );
            return true;
        }
        long rowPosition = scrollStatus.getRowOffset(false);
        if(rowPosition >= 0){
            rowSource.setRowPosition( rowPosition );
            return true;
        }else{
            rowSource.noRow();
            row = 0;
            return false;
        }
    }
	boolean next() throws Exception {
		if(useSetRowPosition) throw SmallSQLException.create(Language.ORDERBY_INTERNAL);
        if(currentInsertedRow() < 0){
    		long rowPosition = scrollStatus.getRowOffset(true);
    		if(rowPosition >= 0){
                row++;
    			rowSource.setRowPosition( rowPosition );
    			return true;
    		}
        }
        if(currentInsertedRow() < insertedRows.size()-1){
            row++;
            rowSource.setRowPosition( insertedRows.get( currentInsertedRow() ) );
            return true;
        }
        if(lastRowOffset >= 0){
            rowSource.setRowPosition( lastRowOffset );
        }else{
            rowSource.beforeFirst();
        }
        if(rowSource.next()){
            row++;
            lastRowOffset = rowSource.getRowPosition();
            insertedRows.add( lastRowOffset );
            return true;
        }
        rowSource.noRow();
        row = (getRowCount() > 0) ? getRowCount() + 1 : 0;
		return false;
	}
	boolean last() throws Exception{
		afterLast();
		return previous();
	}
    final boolean isLast() throws Exception{
        if(row == 0){
            return false;
        }
        if(row > getRowCount()){
            return false;
        }
        boolean isNext = next();
        previous();
        return !isNext;
    }
    final boolean isAfterLast(){
        int rowCount = getRowCount();
        return row > rowCount || rowCount == 0;
    }
	void afterLast() throws Exception{
        useSetRowPosition = false;
        if(sortedRowCount > 0){
            scrollStatus.afterLast();
            scrollStatus.getRowOffset(false); 
        }else{
            rowSource.beforeFirst();
        }
        row = sortedRowCount;
        while(next()){
        }
	}
    boolean absolute(int newRow) throws Exception{
        if(newRow == 0) throw SmallSQLException.create(Language.ROW_0_ABSOLUTE);
        if(newRow > 0){
            beforeFirst();
            while(newRow-- > 0){
                if(!next()){
                    return false;
                }
            }
        }else{
            afterLast();
            while(newRow++ < 0){
                if(!previous()){
                    return false;
                }
            }
        }
        return true;
    }
    boolean relative(int rows) throws Exception{
        if(rows == 0) return (row != 0);
        if(rows > 0){
            while(rows-- > 0){
                if(!next()){
                    return false;
                }
            }
        }else{
            while(rows++ < 0){
                if(!previous()){
                    return false;
                }
            }
        }
        return true;
    }
	int getRow(){
		return row > getRowCount() ? 0 : row;
	}
	final long getRowPosition(){
		return rowSource.getRowPosition();
	}
	final void setRowPosition(long rowPosition) throws Exception{
		rowSource.setRowPosition(rowPosition);
		useSetRowPosition = true;
	}
	final boolean rowInserted(){
		return rowSource.rowInserted();
	}
	final boolean rowDeleted(){
		return rowSource.rowDeleted();
	}
	void nullRow() {
		rowSource.nullRow();
		row = 0;
	}
	void noRow() {
		rowSource.noRow();
		row = 0;
	}
    boolean isExpressionsFromThisRowSource(Expressions columns){
        return rowSource.isExpressionsFromThisRowSource(columns);
    }
    private final int getRowCount(){
        return sortedRowCount + insertedRows.size();
    }
    private final int currentInsertedRow(){
        return row - sortedRowCount - 1;
    }
}
package smallsql.database;
import smallsql.database.language.Language;
class Scrollable extends RowSource {
	private final RowSource rowSource;
	private int rowIdx;
	private final LongList rowList = new LongList();
	Scrollable(RowSource rowSource){
		this.rowSource = rowSource;
	}
	final boolean isScrollable(){
		return true;
	}
	void beforeFirst() throws Exception {
		rowIdx = -1;
		rowSource.beforeFirst();
	}
	boolean isBeforeFirst(){
		return rowIdx == -1 || rowList.size() == 0;
	}
	boolean isFirst(){
		return rowIdx == 0 && rowList.size()>0;
	}
	boolean first() throws Exception {
		rowIdx = -1;
		return next();
	}
	boolean previous() throws Exception{
		if(rowIdx > -1){
			rowIdx--;
			if(rowIdx > -1 && rowIdx < rowList.size()){
				rowSource.setRowPosition( rowList.get(rowIdx) );
				return true;
			}
		}
		rowSource.beforeFirst();
		return false;
	}
	boolean next() throws Exception {
		if(++rowIdx < rowList.size()){
			rowSource.setRowPosition( rowList.get(rowIdx) );
			return true;
		}
		final boolean result = rowSource.next();
		if(result){
			rowList.add( rowSource.getRowPosition());
			return true;
		}
        rowIdx = rowList.size(); 
		return false;
	}
	boolean last() throws Exception{
		afterLast();
		return previous();
	}
	boolean isLast() throws Exception{
        if(rowIdx+1 != rowList.size()){
            return false; 
        }
		boolean isNext = next();
        previous();
        return !isNext && (rowIdx+1 == rowList.size() && rowList.size()>0);
	}
	boolean isAfterLast() throws Exception{
		if(rowIdx >= rowList.size()) return true;
        if(isBeforeFirst() && rowList.size() == 0){
            next();
            previous();
            if(rowList.size() == 0) return true;
        }
        return false;
	}
	void afterLast() throws Exception {
		if(rowIdx+1 < rowList.size()){
			rowIdx = rowList.size()-1;
			rowSource.setRowPosition( rowList.get(rowIdx) );
		}
		while(next()){}
	}
	boolean absolute(int row) throws Exception{
		if(row == 0)
			throw SmallSQLException.create(Language.ROW_0_ABSOLUTE);
		if(row < 0){
			afterLast();
			rowIdx = rowList.size() + row;
			if(rowIdx < 0){
				beforeFirst();
				return false;
			}else{
				rowSource.setRowPosition( rowList.get(rowIdx) );
				return true;
			}
		}
		if(row <= rowList.size()){
			rowIdx = row-1;
			rowSource.setRowPosition( rowList.get(rowIdx) );
			return true;
		}
		rowIdx = rowList.size()-1;
		if(rowIdx >= 0)
			rowSource.setRowPosition( rowList.get(rowIdx) );
		boolean result;
		while((result = next()) && row-1 > rowIdx){}
		return result;
	}
	boolean relative(int rows) throws Exception{
		int newRow = rows + rowIdx + 1;
		if(newRow <= 0){
			beforeFirst();
			return false;
		}else{
			return absolute(newRow);
		}
	}
	int getRow() throws Exception {
        if(rowIdx >= rowList.size()) return 0;
		return rowIdx + 1;
	}
	long getRowPosition() {
		return rowIdx;
	}
	void setRowPosition(long rowPosition) throws Exception {
		rowIdx = (int)rowPosition;
	}
	final boolean rowInserted(){
		return rowSource.rowInserted();
	}
	final boolean rowDeleted(){
		return rowSource.rowDeleted();
	}
	void nullRow() {
		rowSource.nullRow();
		rowIdx = -1;
	}
	void noRow() {
		rowSource.noRow();
		rowIdx = -1;
	}
	void execute() throws Exception{
		rowSource.execute();
		rowList.clear();
		rowIdx = -1;
	}
    boolean isExpressionsFromThisRowSource(Expressions columns){
        return rowSource.isExpressionsFromThisRowSource(columns);
    }
}
package smallsql.database;
import java.sql.*;
import java.util.List;
final class TableResult extends TableViewResult{
    final private Table table;
    private List insertStorePages;
    private long firstOwnInsert; 
    private long maxFileOffset;
	TableResult(Table table){
		this.table = table;
	}
	@Override
    final boolean init( SSConnection con ) throws Exception{
		if(super.init(con)){
			Columns columns = table.columns;
			offsets     = new int[columns.size()];
			dataTypes   = new int[columns.size()];
			for(int i=0; i<columns.size(); i++){
				dataTypes[i] = columns.get(i).getDataType();
			}
			return true;
		}
		return false;
	}
	@Override
    final void execute() throws Exception{
		insertStorePages = table.getInserts(con);
		firstOwnInsert = 0x4000000000000000L | insertStorePages.size();
		maxFileOffset = table.raFile.size();
        beforeFirst();
	}
	@Override
    final TableView getTableView(){
		return table;
	}
	@Override
    final void deleteRow() throws SQLException{
		store.deleteRow(con); 
		store = new StoreNull(store.getNextPagePos());
	}
	@Override
    final void updateRow(Expression[] updateValues) throws Exception{
		Columns tableColumns = table.columns;
		int count = tableColumns.size();
		StoreImpl newStore = table.getStoreTemp(con);
		synchronized(con.getMonitor()){
		    ((StoreImpl)this.store).createWriteLock();
    		for(int i=0; i<count; i++){
    			Expression src = updateValues[i];
    			if(src != null){
    				newStore.writeExpression( src, tableColumns.get(i) );
    			}else{
    				copyValueInto( i, newStore );
    			}
    		}
    		((StoreImpl)this.store).updateFinsh(con, newStore);
		}
	}
	@Override
    final void insertRow(Expression[] updateValues) throws Exception{
		Columns tableColumns = table.columns;
		int count = tableColumns.size();
		StoreImpl store = table.getStoreInsert(con);
		for(int i=0; i<count; i++){
			Column tableColumn = tableColumns.get(i);
			Expression src = updateValues[i];
			if(src == null) src = tableColumn.getDefaultValue(con);
			store.writeExpression( src, tableColumn );
		}
		store.writeFinsh( con );
		insertStorePages.add(store.getLink());
	}
    private Store store = Store.NOROW;
    private long filePos; 
    private int[] offsets;
    private int[] dataTypes;
    private int row;
    private long afterLastValidFilePos;
    final private boolean moveToRow() throws Exception{
    	if(filePos >= 0x4000000000000000L){
    		store = ((StorePageLink)insertStorePages.get( (int)(filePos & 0x3FFFFFFFFFFFFFFFL) )).getStore( table, con, lock);
    	}else{
    		store = (filePos < maxFileOffset) ? table.getStore( con, filePos, lock ) : null;
			if(store == null){
				if(insertStorePages.size() > 0){			
					filePos = 0x4000000000000000L;
					store = ((StorePageLink)insertStorePages.get( (int)(filePos & 0x3FFFFFFFFFFFFFFFL) )).getStore( table, con, lock);
				}
			}
    	}
		if(store != null){
			if(!store.isValidPage()){
				return false;
			}
			store.scanObjectOffsets( offsets, dataTypes );
			afterLastValidFilePos = store.getNextPagePos();
			return true;
		}else{
			filePos = -1;
			noRow();
			return false;
		}
    }
    final private boolean moveToValidRow() throws Exception{
		while(filePos >= 0){
        	if(moveToRow())
        		return true;
			setNextFilePos();
    	}
        row = 0;
    	return false;
    }
	@Override
    final void beforeFirst(){
		filePos = 0;
		store = Store.NOROW;
		row = 0;
	}
	@Override
    final boolean first() throws Exception{
		filePos = table.getFirstPage();
		row = 1;
		return moveToValidRow();
	}
	final private void setNextFilePos(){
		if(filePos < 0) return; 
		if(store == Store.NOROW)
			 filePos = table.getFirstPage(); 
		else
		if(filePos >= 0x4000000000000000L){
			filePos++;
			if((filePos & 0x3FFFFFFFFFFFFFFFL) >= insertStorePages.size()){
				filePos = -1;
				noRow();
			}
		}else
			filePos = store.getNextPagePos();
	}
    @Override
    final boolean next() throws Exception{
        if(filePos < 0) return false;
		setNextFilePos();
        row++;
        return moveToValidRow();
    }
	@Override
    final void afterLast(){
		filePos = -1;
		noRow();
	}
	@Override
    final int getRow(){
    	return row;
    }
	@Override
    final long getRowPosition(){
		return filePos;
	}
	@Override
    final void setRowPosition(long rowPosition) throws Exception{
		filePos = rowPosition;
		if(filePos < 0 || !moveToRow()){
			store = new StoreNull(store.getNextPagePos());
		}
	}
	@Override
    final boolean rowInserted(){
		return filePos >= firstOwnInsert;
	}
	@Override
    final boolean rowDeleted(){
		if(store instanceof StoreNull && store != Store.NULL){
            return true;
        }
        if(store instanceof StoreImpl &&
            ((StoreImpl)store).isRollback()){
            return true;
        }
        return false;
	}
	@Override
    final void nullRow(){
		row = 0;
    	store = Store.NULL;
    }
	@Override
    final void noRow(){
		row = 0;
		store = Store.NOROW;
	}
	@Override
    final boolean isNull( int colIdx ) throws Exception{
        return store.isNull( offsets[colIdx] );
    }
	@Override
    final boolean getBoolean( int colIdx ) throws Exception{
        return store.getBoolean( offsets[colIdx], dataTypes[colIdx] );
    }
	@Override
    final int getInt( int colIdx ) throws Exception{
        return store.getInt( offsets[colIdx], dataTypes[colIdx] );
    }
	@Override
    final long getLong( int colIdx ) throws Exception{
        return store.getLong( offsets[colIdx], dataTypes[colIdx] );
    }
	@Override
    final float getFloat( int colIdx ) throws Exception{
        return store.getFloat( offsets[colIdx], dataTypes[colIdx] );
    }
	@Override
    final double getDouble( int colIdx ) throws Exception{
        return store.getDouble( offsets[colIdx], dataTypes[colIdx] );
    }
	@Override
    final long getMoney( int colIdx ) throws Exception{
        return store.getMoney( offsets[colIdx], dataTypes[colIdx] );
    }
	@Override
    final MutableNumeric getNumeric( int colIdx ) throws Exception{
        return store.getNumeric( offsets[colIdx], dataTypes[colIdx] );
    }
	@Override
    final Object getObject( int colIdx ) throws Exception{
        return store.getObject( offsets[colIdx], dataTypes[colIdx] );
    }
	@Override
    final String getString( int colIdx ) throws Exception{
        return store.getString( offsets[colIdx], dataTypes[colIdx] );
    }
	@Override
    final byte[] getBytes( int colIdx ) throws Exception{
        return store.getBytes( offsets[colIdx], dataTypes[colIdx] );
    }
	@Override
    final int getDataType( int colIdx ){
        return dataTypes[colIdx];
    }
    final private void copyValueInto( int colIdx, StoreImpl dst){
    	int offset = offsets[colIdx++];
    	int length = (colIdx < offsets.length ? offsets[colIdx] : store.getUsedSize()) - offset;
		dst.copyValueFrom( (StoreImpl)store, offset, length);
    }
}
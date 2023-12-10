package smallsql.database;
import java.sql.SQLException;
import smallsql.database.language.Language;
class IndexDescriptions {
	private int size;
	private IndexDescription[] data;
    private boolean hasPrimary;
	IndexDescriptions(){
		data = new IndexDescription[4];
	}
	final int size(){
		return size;
	}
	final IndexDescription get(int idx){
		if (idx >= size)
			throw new IndexOutOfBoundsException("Column index: "+idx+", Size: "+size);
		return data[idx];
	}
	final void add(IndexDescription descr) throws SQLException{
		if(size >= data.length ){
			resize(size << 1);
		}
        if(hasPrimary && descr.isPrimary()){
            throw SmallSQLException.create(Language.PK_ONLYONE);
        }
        hasPrimary = descr.isPrimary();
		data[size++] = descr;
	}
	private final void resize(int newSize){
		IndexDescription[] dataNew = new IndexDescription[newSize];
		System.arraycopy(data, 0, dataNew, 0, size);
		data = dataNew;		
	}
	final IndexDescription findBestMatch(Strings columns){
		int bestFactor = Integer.MAX_VALUE;
		int bestIdx = 0;
		for(int i=0; i<size; i++){
			int factor = data[i].matchFactor(columns);
			if(factor == 0) 
				return data[i];
			if(factor < bestFactor){
				bestFactor = factor;
				bestIdx = i;
			}
		}
		if(bestFactor == Integer.MAX_VALUE)
			return null;
		else
			return data[bestIdx];
	}
	void create(SSConnection con, Database database, TableView tableView) throws Exception{
		for(int i=0; i<size; i++){
			data[i].create(con, database, tableView);
		}
	}
	void drop(Database database) throws Exception {
		for(int i=0; i<size; i++){
			data[i].drop(database);
		}
	}
    void close() throws Exception{
        for(int i=0; i<size; i++){
            data[i].close();
        }
    }
    void add(IndexDescriptions indexes) throws SQLException {
        for(int i=0; i<indexes.size; i++){
            add(indexes.data[i]);
        }
    }
}
package smallsql.database;
final class DataSources {
	private int size;
	private DataSource[] data = new DataSource[4];
	final int size(){
		return size;
	}
	final DataSource get(int idx){
		if (idx >= size)
			throw new IndexOutOfBoundsException("Index: "+idx+", Size: "+size);
		return data[idx];
	}
	final void add(DataSource table){
		if(size >= data.length ){
			DataSource[] dataNew = new DataSource[size << 1];
			System.arraycopy(data, 0, dataNew, 0, size);
			data = dataNew;
		}
		data[size++] = table;
	}
}
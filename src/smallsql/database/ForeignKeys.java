package smallsql.database;
class ForeignKeys {
	private int size;
	private ForeignKey[] data;
	ForeignKeys(){
		data = new ForeignKey[16];
	}
	final int size(){
		return size;
	}
	final ForeignKey get(int idx){
		if (idx >= size)
			throw new IndexOutOfBoundsException("Column index: "+idx+", Size: "+size);
		return data[idx];
	}
	final void add(ForeignKey foreignKey){
		if(size >= data.length ){
			resize(size << 1);
		}
		data[size++] = foreignKey;
	}
	private final void resize(int newSize){
		ForeignKey[] dataNew = new ForeignKey[newSize];
		System.arraycopy(data, 0, dataNew, 0, size);
		data = dataNew;		
	}
}
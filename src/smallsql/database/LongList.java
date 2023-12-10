package smallsql.database;
class LongList {
	private int size;
	private long[] data;
	LongList(){
		this(16);
	}
	LongList(int initialSize){
		data = new long[initialSize];
	}
	final int size(){
		return size;
	}
	final long get(int idx){
		if (idx >= size)
			throw new IndexOutOfBoundsException("Index: "+idx+", Size: "+size);
		return data[idx];
	}
	final void add(long value){
		if(size >= data.length ){
			resize(size << 1);
		}
		data[ size++ ] = value;
	}
	final void clear(){
		size = 0;
	}
	private final void resize(int newSize){
		long[] dataNew = new long[newSize];
		System.arraycopy(data, 0, dataNew, 0, size);
		data = dataNew;		
	}
}
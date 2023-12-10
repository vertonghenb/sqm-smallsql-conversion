package smallsql.database;
final class LongLongList {
	private int size;
	private long[] data;
	LongLongList(){
		this(16);
	}
	LongLongList(int initialSize){
		data = new long[initialSize*2];
	}
	final int size(){
		return size;
	}
	final long get1(int idx){
		if (idx >= size)
			throw new IndexOutOfBoundsException("Index: "+idx+", Size: "+size);
		return data[idx << 1];
	}
	final long get2(int idx){
		if (idx >= size)
			throw new IndexOutOfBoundsException("Index: "+idx+", Size: "+size);
		return data[(idx << 1) +1];
	}
	final void add(long value1, long value2){
		int size2 = size << 1;
		if(size2 >= data.length ){
			resize(size2);
		}
		data[ size2   ] = value1;
		data[ size2 +1] = value2;
		size++;
	}
	final void clear(){
		size = 0;
	}
	private final void resize(int newSize){
		long[] dataNew = new long[newSize << 1];
		System.arraycopy(data, 0, dataNew, 0, size << 1);
		data = dataNew;		
	}
}
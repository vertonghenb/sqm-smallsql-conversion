package smallsql.database;
class Strings {
	private int size;
	private String[] data;
	Strings(){
		data = new String[16];
	}
	final int size(){
		return size;
	}
	final String get(int idx){
		if (idx >= size)
			throw new IndexOutOfBoundsException("Column index: "+idx+", Size: "+size);
		return data[idx];
	}
	final void add(String descr){
		if(size >= data.length ){
			resize(size << 1);
		}
		data[size++] = descr;
	}
	private final void resize(int newSize){
		String[] dataNew = new String[newSize];
		System.arraycopy(data, 0, dataNew, 0, size);
		data = dataNew;		
	}
    public String[] toArray() {
        String[] array = new String[size];
        System.arraycopy(data, 0, array, 0, size);
        return array;     
    }
}
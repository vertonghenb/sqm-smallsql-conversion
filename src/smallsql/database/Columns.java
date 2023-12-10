package smallsql.database;
final class Columns {
	private int size;
	private Column[] data;
	Columns(){
		data = new Column[16];
	}
	final int size(){
		return size;
	}
	final Column get(int idx){
		if (idx >= size)
			throw new IndexOutOfBoundsException("Column index: "+idx+", Size: "+size);
		return data[idx];
	}
    final Column get(String name){
        for(int i = 0; i < size; i++){
            Column column = data[i];
            if(name.equalsIgnoreCase(column.getName())){
                return column;
            }
        }
        return null;
    }
    final void add(Column column){
        if(column == null){
            throw new NullPointerException("Column is null.");
        }
        if(size >= data.length){
            resize(size << 1);
        }
        data[size++] = column;
    }
    Columns copy(){
        Columns copy = new Columns();
        Column[] cols = copy.data = (Column[]) data.clone(); 
        for(int i=0; i<size; i++){
            cols[i] = cols[i].copy();
        }
        copy.size = size;
        return copy;
    }
	private final void resize(int newSize){
		Column[] dataNew = new Column[newSize];
		System.arraycopy(data, 0, dataNew, 0, size);
		data = dataNew;		
	}
}
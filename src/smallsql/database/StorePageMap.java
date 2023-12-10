package smallsql.database;
class StorePageMap {
	private Entry[] table;
	private int size;
	private int threshold;
	StorePageMap() {
		threshold = 12;
		table = new Entry[17];
	}
	final int size() {
		return size;
	}
	final boolean isEmpty() {
		return size == 0;
	}
	final TableStorePage get(long key) {
		int i = (int)(key % table.length);
		Entry e = table[i]; 
		while (true) {
			if (e == null)
				return null;
			if (e.key == key) 
				return e.value;
			e = e.next;
		}
	}
	final boolean containsKey(long key) {
		return (get(key) != null);
	}
	final TableStorePage add(long key, TableStorePage value) {
		int i = (int)(key % table.length);
		table[i] = new Entry(key, value, table[i]);
		if (size++ >= threshold) 
			resize(2 * table.length);
		return null;
	}
	final private void resize(int newCapacity) {
		Entry[] newTable = new Entry[newCapacity];
		transfer(newTable);
		table = newTable;
		threshold = (int)(newCapacity * 0.75f);
	}
	final private void transfer(Entry[] newTable) {
		Entry[] src = table;
		int newCapacity = newTable.length;
		for (int j = 0; j < src.length; j++) {
			Entry e = src[j];
			if (e != null) {
				src[j] = null;
				do {
					Entry next = e.next;
					e.next = null;
					int i = (int)(e.key % newCapacity);
					if(newTable[i] == null){
						newTable[i] = e;
					}else{
						Entry entry = newTable[i];
						while(entry.next != null) entry = entry.next;
						entry.next = e;
					}
					e = next;
				} while (e != null);
			}
		}
	}
	final TableStorePage remove(long key) {
		int i = (int)(key % table.length);
		Entry prev = table[i];
		Entry e = prev;
		while (e != null) {
			Entry next = e.next;
			if (e.key == key) {
				size--;
				if (prev == e) 
					table[i] = next;
				else
					prev.next = next;
				return e.value;
			}
			prev = e;
			e = next;
		}
		return null;
	}
	final void clear() {
		Entry tab[] = table;
		for (int i = 0; i < tab.length; i++) 
			tab[i] = null;
		size = 0;
	}
	final boolean containsValue(TableStorePage value) {
		Entry tab[] = table;
			for (int i = 0; i < tab.length ; i++)
				for (Entry e = tab[i] ; e != null ; e = e.next)
					if (value.equals(e.value))
						return true;
		return false;
	}
	static class Entry{
		final long key;
		final TableStorePage value;
		Entry next;
		Entry(long k, TableStorePage v, Entry n) { 
			value = v; 
			next = n;
			key = k;
		}
	}
}
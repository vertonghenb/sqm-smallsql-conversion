package smallsql.database;
final class MutableLong extends Number implements Mutable{
	long value;
	MutableLong(long value){
		this.value = value;
	}
	public double doubleValue() {
		return value;
	}
	public float floatValue() {
		return value;
	}
	public int intValue() {
		return (int)value;
	}
	public long longValue() {
		return value;
	}
	public String toString(){
		return String.valueOf(value);
	}
	public Object getImmutableObject(){
		return new Long(value);
	}
}
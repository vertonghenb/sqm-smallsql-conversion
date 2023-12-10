package smallsql.database;
final class MutableInteger extends Number implements Mutable{
	int value;
	MutableInteger(int value){
		this.value = value;
	}
	public double doubleValue() {
		return value;
	}
	public float floatValue() {
		return value;
	}
	public int intValue() {
		return value;
	}
	public long longValue() {
		return value;
	}
	public String toString(){
		return String.valueOf(value);
	}
	public Object getImmutableObject(){
		return Utils.getInteger(value);
	}
}
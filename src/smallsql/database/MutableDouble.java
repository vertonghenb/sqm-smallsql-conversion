package smallsql.database;
final class MutableDouble extends Number implements Mutable{
	double value;
	MutableDouble(double value){
		this.value = value;
	}
	public double doubleValue() {
		return value;
	}
	public float floatValue() {
		return (float)value;
	}
	public int intValue() {
		return (int)value;
	}
	public long longValue() {
		return (long)value;
	}
	public String toString(){
		return String.valueOf(value);
	}
	public Object getImmutableObject(){
		return new Double(value);
	}
}
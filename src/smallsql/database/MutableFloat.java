package smallsql.database;
final class MutableFloat extends Number implements Mutable{
	float value;
	MutableFloat(float value){
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
		return (long)value;
	}
	public String toString(){
		return String.valueOf(value);
	}
	public Object getImmutableObject(){
		return new Float(value);
	}
}
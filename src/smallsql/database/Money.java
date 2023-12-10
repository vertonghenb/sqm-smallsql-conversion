package smallsql.database;
import java.math.*;
public class Money extends Number implements Mutable{
    private static final long serialVersionUID = -620300937494609089L;
    long value;
    private Money(){}
    public Money(double value){
        this.value = (long)(value * 10000);
    }
    public Money(float value){
        this.value = (long)(value * 10000);
    }
    public static Money createFromUnscaledValue(long value){
        Money money = new Money();
        money.value = value;
        return money;
    }
    public static Money createFromUnscaledValue(int value){
        Money money = new Money();
        money.value = value;
        return money;
    }
    public int intValue() {
        return (int)(value / 10000.0);
    }
    public float floatValue() {
        return value / 10000.0F;
    }
    public double doubleValue() {
        return value / 10000.0;
    }
    public long longValue() {
        return (long)(value / 10000.0);
    }
    public String toString(){
		StringBuffer buffer = new StringBuffer();
		buffer.append(longValue()).append('.');
		final long v = Math.abs(value);
		buffer.append( (char)((v % 10000) / 1000 + '0') );
		buffer.append( (char)((v % 1000) / 100 + '0') );
		buffer.append( (char)((v % 100) / 10 + '0') );
		buffer.append( (char)((v % 10) + '0') );
        return buffer.toString();
    }
    public boolean equals(Object obj){
        return (obj instanceof Money && ((Money)obj).value == value);
    }
    public int hashCode(){
        return (int)(value ^ (value >>> 32));
    }
    public long unscaledValue(){
        return value;
    }
    public static long parseMoney( String str ){
        return Utils.doubleToMoney(Double.parseDouble( str ));
    }
    private byte[] toByteArray(){
    	byte[] bytes = new byte[8];
		int offset = 0;
		bytes[offset++] = (byte)(value >> 56);
		bytes[offset++] = (byte)(value >> 48);
		bytes[offset++] = (byte)(value >> 40);
		bytes[offset++] = (byte)(value >> 32);
		bytes[offset++] = (byte)(value >> 24);
		bytes[offset++] = (byte)(value >> 16);
		bytes[offset++] = (byte)(value >> 8);
		bytes[offset++] = (byte)(value);
    	return bytes;
    }
	public BigDecimal toBigDecimal(){
		if(value == 0) return ZERO;
		return new BigDecimal( new BigInteger( toByteArray() ), 4 );
	}
	public Object getImmutableObject(){
		return toBigDecimal();
	}
	static private final BigDecimal ZERO = new BigDecimal("0.0000");
}
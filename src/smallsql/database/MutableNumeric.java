package smallsql.database;
import java.math.*;
class MutableNumeric extends Number implements Mutable{
    private static final long serialVersionUID = -750525164208565056L;
    private int[] value;
    private int scale;
    private int signum;
    MutableNumeric(byte[] complement){
		setValue(complement);
    }
    private void setValue(byte[] complement){
        int length = complement.length;
        if(length == 0){
            value   = EMPTY_INTS;
            signum  = 0;
            return;
        }
        value = new int[ (length + 3) / 4 ];
        if(complement[0] < 0){
            negate( complement );
            signum = -1;
        }else{
			signum = 0;
        	for(int i=0; i<complement.length; i++)
        		if(complement[i] != 0){
        			signum = 1;
        			break;
        		}
        }
        for(int v=value.length-1; v>=0; v--){
            int temp = 0;
            for(int i=0; i<4 && 0<length; i++){
                temp |= (complement[ --length ] & 0xFF) << (i*8);
            }
            value[v] = temp;
        }
    }
    MutableNumeric(int complement){
        if(complement == 0){
            signum = 0;
            value = EMPTY_INTS;
        }else{
            value = new int[1];
            if(complement < 0){
                value[0] = -complement;
                signum = -1;
            }else{
                value[0] = complement;
                signum = 1;
            }
        }
    }
    MutableNumeric(int complement, int scale){
        this( complement );
        this.scale = scale;
    }
    MutableNumeric(long complement){
        if(complement == 0){
            signum = 0;
            value = EMPTY_INTS;
        }else{
            value = new int[2];
            if(complement < 0){
                value[0] = (int)(~(complement >> 32));
                value[1] = (int)(-complement);
                signum = -1;
            }else{
                value[0] = (int)(complement >> 32);
                value[1] = (int)complement;
                signum = 1;
            }
        }
    }
    MutableNumeric(long complement, int scale){
        this( complement );
        this.scale = scale;
    }
    MutableNumeric(double val){
        this( new BigDecimal( String.valueOf(val) ) );
    }
    MutableNumeric(float val){
        this( new BigDecimal( String.valueOf(val) ) );
    }
    MutableNumeric(String val){
        this( new BigDecimal( val ) );
    }
    MutableNumeric( BigDecimal big ){
        this(big.unscaledValue().toByteArray() );
        scale   = big.scale();
    }
    MutableNumeric(int signum, int[] value, int scale){
        this.signum = signum;
        this.value  = value;
        this.scale  = scale;
    }
	MutableNumeric(MutableNumeric numeric){
		this.signum = numeric.signum;
		this.value  = new int[numeric.value.length];
		System.arraycopy(numeric.value, 0, value, 0, value.length);
		this.scale  = numeric.scale;
	}
    int[] getInternalValue(){
        return value;
    }
    void add(MutableNumeric num){
		if(num.scale < scale){
			num.setScale(scale);
		}else
		if(num.scale > scale){
			setScale(num.scale);
		}
        add( num.signum, num.value );
    }
    private void add( int sig2, int[] val2){
        if(val2.length > value.length){
            int[] temp = val2;
            val2 = value;
            value = temp;
            int tempi = signum;
            signum = sig2;
            sig2 = tempi;
        }
        if(signum != sig2)
            sub(val2);
        else
            add(val2);
    }
    private void add( int[] val2){
        long temp = 0;
        int v1 = value.length;
        for(int v2 = val2.length; v2>0; ){
            temp = (value[--v1] & 0xFFFFFFFFL) + (val2 [--v2] & 0xFFFFFFFFL) + (temp >>> 32);
            value[v1] = (int)temp;
        }
        boolean uebertrag = (temp >>> 32) != 0;
        while(v1 > 0 && uebertrag)
            uebertrag = (value[--v1] = value[v1] + 1) == 0;
        if(uebertrag){
			resizeValue(1);
        }
    }
	private void resizeValue(int highBits){
		int val[] = new int[value.length+1];
		val[0] = highBits;
		System.arraycopy(value, 0, val, 1, value.length);
		value = val;
    }
    void sub(MutableNumeric num){
		if(num.scale < scale){
			num.setScale(scale);
		}else
		if(num.scale > scale){
			setScale(num.scale);
		}
        add( -num.signum, num.value );
    }
    private void sub(int[] val2){
        long temp = 0;
        int v1 = value.length;
        for(int v2 = val2.length; v2>0; ){
            temp = (value[--v1] & 0xFFFFFFFFL) - (val2 [--v2] & 0xFFFFFFFFL) + (temp >>>= 32);
            value[v1] = (int)temp;
        }
        boolean uebertrag = (temp >>> 32) != 0;
        while(v1 > 0 && uebertrag)
            uebertrag = (value[--v1] = value[v1] - 1) == -1;
        if(uebertrag){
            signum = -signum;
            int last = value.length-1;
            for(int i=0; i<=last; i++){
                value[i] = (i == last) ? -value[i] : ~value[i];
            }
        }
    }
    void mul(MutableNumeric num){
		BigDecimal big = toBigDecimal().multiply(num.toBigDecimal() );
		setValue( big.unscaledValue().toByteArray() );
		scale = big.scale();
		signum = big.signum();
    }
	final void mul(int factor){
		if(factor < 0){
			factor = - factor;
			signum = -signum;
		}
		long carryover = 0;
		for(int i = value.length-1; i>=0; i--){
			long v = (value[i] & 0xFFFFFFFFL) * factor + carryover;
			value[i] = (int)v;
			carryover = v >> 32;
		}
		if(carryover > 0){
			resizeValue( (int)carryover );
		}
	}
    void div(MutableNumeric num){
		int newScale = Math.max(scale+5, num.scale +4);
		BigDecimal big = toBigDecimal().divide(num.toBigDecimal(), newScale, BigDecimal.ROUND_HALF_EVEN);
		setValue( big.unscaledValue().toByteArray() );
		scale = big.scale();
		signum = big.signum();
    }
	final void div(int quotient){
		mul(100000);
		scale += 5;
		divImpl(quotient);
	}
	final private void divImpl(int quotient){	
		if(quotient == 1) return;
		if(quotient < 0){
			quotient = - quotient;
			signum = -signum;
		}
		int valueLength = value.length;
		long carryover = 0;
		for(int i = 0; i<valueLength; i++){
			long v = (value[i] & 0xFFFFFFFFL) + carryover;
			value[i] = (int)(v / quotient);
			carryover = ((v % quotient) << 32);
		}
		carryover /= quotient;
		if(carryover > 2147483648L || 
		  (carryover == 2147483648L && (value[valueLength-1] % 2 == 1))){
			int i = valueLength-1;
			boolean isCarryOver = true;
			while(i >= 0 && isCarryOver)
				isCarryOver = (value[i--] += 1) == 0;
		}
		if(valueLength>1 && value[0] == 0){
			int[] temp = new int[valueLength-1];
			System.arraycopy(value, 1, temp, 0, valueLength-1);
			value = temp;
		}
	}
    void mod(MutableNumeric num){
		num = new MutableNumeric( doubleValue() % num.doubleValue() );
		value = num.value;
		scale = num.scale;
		signum = num.signum;
    }
	int getScale(){
	    return scale;
    }
	void setScale(int newScale){
		if(newScale == scale) return;
		int factor = 1;
		if(newScale > scale){
			for(;newScale>scale; scale++){
				factor *=10;
				if(factor == 1000000000){
					mul(factor);
					factor = 1;
				}
			}
			mul(factor);
		}else{
			for(;newScale<scale; scale--){
				factor *=10;
				if(factor == 1000000000){
					divImpl(factor);
					factor = 1;
				}
			}
			divImpl(factor);		
		}
	}
    int getSignum() {
        return signum;
    }
    void setSignum(int signum){
        this.signum = signum;
    }
    void floor(){
		int oldScale = scale;
		setScale(0);
		setScale(oldScale);
	}
    private void negate(byte[] complement){
        int last = complement.length-1;
        for(int i=0; i<=last; i++){
            complement[i] = (byte)( (i == last) ? -complement[i] : ~complement[i]);
        }
        while(complement[last] == 0){
            last--;
            complement[last]++;
        }
    }
    byte[] toByteArray(){
        if(signum == 0) return EMPTY_BYTES;
        byte[] complement;
        int offset;
        int v = 0;
        while(v < value.length && value[v] == 0) v++;
        if (v == value.length) return EMPTY_BYTES;
        if(value[v] < 0){
            complement = new byte[(value.length-v)*4 + 4];
            if(signum < 0)
                complement[0] = complement[1] = complement[2] = complement[3] = -1;
            offset = 4;
        }else{
            complement = new byte[(value.length-v)*4];
            offset = 0;
        }
        int last = value.length-1;
        for(; v <= last; v++){
            int val = (signum>0) ? value[v] : (v == last) ? -value[v] : ~value[v];
            complement[offset++] = (byte)(val >> 24);
            complement[offset++] = (byte)(val >> 16);
            complement[offset++] = (byte)(val >> 8);
            complement[offset++] = (byte)(val);
        }
        return complement;
    }
    public int intValue(){
        return Utils.long2int(longValue());
    }
    public long longValue(){
        if(value.length == 0 || signum == 0){
            return 0;
        }else{
            if (value.length == 1 && (value[0] > 0)){
                return Utils.double2long(value[0] / scaleDoubleFactor[scale] * signum);
            }else
            if (value.length == 1){
                long temp = value[0] & 0xFFFFFFFFL;
                return Utils.double2long(temp / scaleDoubleFactor[scale] * signum);
            }else
            if (value.length == 2 && (value[0] > 0)){
                long temp = (((long)value[0]) << 32) | (value[1] & 0xFFFFFFFFL);
                return Utils.double2long(temp / scaleDoubleFactor[scale] * signum);
            }else{
           		if(scale != 0){
           			MutableNumeric numeric = new MutableNumeric(this);
           			numeric.setScale(0);
           			return numeric.longValue();
           		}           			
            	return (signum > 0) ? Long.MAX_VALUE : Long.MIN_VALUE;
            }
        }
    }
    public float floatValue(){
        if(value.length == 0 || signum == 0){
            return 0;
        }else{
            if (value.length == 1 && (value[0] > 0)){
                return value[0] / scaleFloatFactor[scale] * signum;
            }else
            if (value.length == 1){
                long temp = value[0] & 0xFFFFFFFFL;
                return temp / scaleFloatFactor[scale] * signum;
            }else
            if (value.length == 2 && (value[0] > 0)){
                long temp = (((long)value[0]) << 32) | (value[1] & 0xFFFFFFFFL);
                return temp / scaleFloatFactor[scale] * signum;
            }else{
                return new BigDecimal( new BigInteger( toByteArray() ), scale ).floatValue();
            }
        }
    }
    public double doubleValue(){
        if(value.length == 0 || signum == 0){
            return 0;
        }else{
            if (value.length == 1 && (value[0] > 0)){
                return value[0] / scaleDoubleFactor[scale] * signum;
            }else
            if (value.length == 1){
                long temp = value[0] & 0xFFFFFFFFL;
                return temp / scaleDoubleFactor[scale] * signum;
            }else
            if (value.length == 2 && (value[0] > 0)){
                long temp = (((long)value[0]) << 32) | (value[1] & 0xFFFFFFFFL);
                return temp / scaleDoubleFactor[scale] * signum;
            }else{
                return new BigDecimal( new BigInteger( toByteArray() ), scale ).doubleValue();
            }
        }
    }
    public String toString(){
        StringBuffer buf = new StringBuffer();
        if(value.length == 0 || signum == 0){
            buf.append( '0' );
        }else{
            if (value.length == 1 && (value[0] > 0)){
                buf.append( Integer.toString(value[0]) );
            }else
            if (value.length == 1){
                long temp = value[0] & 0xFFFFFFFFL;
                buf.append( Long.toString( temp ) );
            }else
            if (value.length == 2 && (value[0] > 0)){
                long temp = (((long)value[0]) << 32) | (value[1] & 0xFFFFFFFFL);
                buf.append( Long.toString( temp ) );
            }else{
                return new BigDecimal( new BigInteger( toByteArray() ), scale ).toString();
            }
        }
        if(scale > 0){
            while(buf.length() <= scale) buf.insert( 0, '0' );
            buf.insert( buf.length() - scale, '.' );
        }
        if (signum < 0) buf.insert( 0, '-');
        return buf.toString();
    }
    public int compareTo(MutableNumeric numeric){
		return toBigDecimal().compareTo(numeric.toBigDecimal());
    }           
	public boolean equals(Object obj){
		if(!(obj instanceof MutableNumeric)) return false;
		return compareTo((MutableNumeric)obj) == 0;
	}
    public BigDecimal toBigDecimal(){
		if(signum == 0) return new BigDecimal( BigInteger.ZERO, scale);
        return new BigDecimal( new BigInteger( toByteArray() ), scale );
    }
    public BigDecimal toBigDecimal(int newScale){
        if(newScale == this.scale) return toBigDecimal();
        return toBigDecimal().setScale( newScale, BigDecimal.ROUND_HALF_EVEN);
    }
	public Object getImmutableObject(){
		return toBigDecimal();
	}
    private static final byte[] EMPTY_BYTES = new byte[0];
    private static final int [] EMPTY_INTS  = new int [0];
    private static final double[] scaleDoubleFactor = { 1, 10, 100, 1000, 10000, 100000, 1000000, 10000000 };
    private static final float[]  scaleFloatFactor =  { 1, 10, 100, 1000, 10000, 100000, 1000000, 10000000 };
}
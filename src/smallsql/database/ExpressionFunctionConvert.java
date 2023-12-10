package smallsql.database;
import java.util.Arrays;
import smallsql.database.language.Language;
public class ExpressionFunctionConvert extends ExpressionFunction {
	final private Column datatype;
	public ExpressionFunctionConvert(Column datatype, Expression value, Expression style) {
		super();
		this.datatype = datatype;
		Expression[] params = (style == null) ? new Expression[]{value} : new Expression[]{value, style};
		setParams( params );
	}
	int getFunction() {
		return SQLTokenizer.CONVERT;
	}
	boolean isNull() throws Exception {
		return param1.isNull();
	}
	boolean getBoolean() throws Exception {
		return ExpressionValue.getBoolean( getObject(), getDataType() );
	}
	int getInt() throws Exception {
		return ExpressionValue.getInt( getObject(), getDataType() );
	}
	long getLong() throws Exception {
		return ExpressionValue.getLong( getObject(), getDataType() );
	}
	float getFloat() throws Exception {
		return ExpressionValue.getFloat( getObject(), getDataType() );
	}
	double getDouble() throws Exception {
		return ExpressionValue.getDouble( getObject(), getDataType() );
	}
	long getMoney() throws Exception {
		return ExpressionValue.getMoney(getObject(), getDataType());
	}
	MutableNumeric getNumeric() throws Exception {
		return ExpressionValue.getNumeric(getObject(), getDataType());
	}
	String getString() throws Exception {
		Object obj = getObject();
		if(obj == null) return null;
		switch(datatype.getDataType()){
			case SQLTokenizer.BIT:
				return ((Boolean)obj).booleanValue() ? "1" : "0";
            case SQLTokenizer.BINARY:
            case SQLTokenizer.VARBINARY:
            case SQLTokenizer.LONGVARBINARY:
                    return new String( (byte[])obj );
		}
		return obj.toString();
	}
	Object getObject() throws Exception {
		if(param1.isNull()) return null;
		final int dataType = getDataType();
		switch(dataType){
			case SQLTokenizer.LONGVARCHAR:
				return convertToString();
			case SQLTokenizer.VARCHAR:{
				String str = convertToString();
				int length = datatype.getDisplaySize();
				if(length > str.length())
					return str;
				return str.substring(0,length);
			}
			case SQLTokenizer.CHAR:{
				String str = convertToString();
				int length = datatype.getDisplaySize();
				if(length > str.length()){
					char[] buffer = new char[length-str.length()];
					Arrays.fill(buffer, ' ');
					return str + new String(buffer);
				}
				return str.substring(0,length);
			}
			case SQLTokenizer.LONGVARBINARY:
				return param1.getBytes();
			case SQLTokenizer.VARBINARY:{
				byte[] bytes = param1.getBytes();
				int length = datatype.getPrecision();
				if(length < bytes.length){
					byte[] buffer = new byte[length];
					System.arraycopy(bytes, 0, buffer, 0, Math.min(bytes.length,length) );
					return buffer;
				}
				return bytes;
			}
			case SQLTokenizer.BINARY:{
				byte[] bytes = param1.getBytes();
				int length = datatype.getPrecision();
				if(length != bytes.length){
					byte[] buffer = new byte[length];
					System.arraycopy(bytes, 0, buffer, 0, Math.min(bytes.length,length) );
					return buffer;
				}
				return bytes;
			}
			case SQLTokenizer.BOOLEAN:
			case SQLTokenizer.BIT:
				return param1.getBoolean() ? Boolean.TRUE : Boolean.FALSE;
			case SQLTokenizer.TINYINT:
				return Utils.getInteger(param1.getInt() & 0xFF);
			case SQLTokenizer.SMALLINT:
				return Utils.getInteger((short)param1.getInt());
			case SQLTokenizer.INT:
				return Utils.getInteger(param1.getInt());
			case SQLTokenizer.BIGINT:
				return new Long(param1.getLong());
			case SQLTokenizer.REAL:
				return new Float(param1.getFloat());
			case SQLTokenizer.FLOAT:
			case SQLTokenizer.DOUBLE:
				return new Double(param1.getDouble());
			case SQLTokenizer.DATE:
			case SQLTokenizer.TIME:
			case SQLTokenizer.TIMESTAMP:
			case SQLTokenizer.SMALLDATETIME:
				return new DateTime( getDateTimeLong(), dataType );
			case SQLTokenizer.NUMERIC:
			case SQLTokenizer.DECIMAL:
				MutableNumeric num = param1.getNumeric();
				if(num != null && (dataType == SQLTokenizer.NUMERIC || dataType == SQLTokenizer.DECIMAL))
					num.setScale(getScale());
				return num;
			case SQLTokenizer.MONEY:
			case SQLTokenizer.SMALLMONEY:
				return Money.createFromUnscaledValue(param1.getMoney());
			case SQLTokenizer.UNIQUEIDENTIFIER:
				switch(param1.getDataType()){
					case SQLTokenizer.VARCHAR:
					case SQLTokenizer.CHAR:
					case SQLTokenizer.LONGVARCHAR:
					case SQLTokenizer.CLOB:
						return Utils.bytes2unique( Utils.unique2bytes(param1.getString()), 0);
				}
				return Utils.bytes2unique(param1.getBytes(), 0);
		}
		Object[] param = { SQLTokenizer.getKeyWord(dataType) };
		throw SmallSQLException.create(Language.UNSUPPORTED_TYPE_CONV, param);
	}
	final private String convertToString() throws Exception{
		if(param2 != null){
			int type = param1.getDataType();
			switch(type){
				case SQLTokenizer.SMALLDATETIME:
					type = SQLTokenizer.TIMESTAMP;
				case SQLTokenizer.TIMESTAMP:
				case SQLTokenizer.DATE:
				case SQLTokenizer.TIME:
					return new DateTime( param1.getLong(), type ).toString(param2.getInt());
				default:
					return param1.getString();
			}
		}else
			return param1.getString();
	}
	final private long getDateTimeLong() throws Exception{
			switch(param1.getDataType()){
				case SQLTokenizer.LONGVARCHAR:
				case SQLTokenizer.VARCHAR:
				case SQLTokenizer.CHAR:
					return DateTime.parse( param1.getString() );
			}
		return param1.getLong();
	}
	final int getDataType() {
		return datatype.getDataType();
	}
	final int getPrecision(){
		final int dataType = getDataType();
		switch(dataType){
			case SQLTokenizer.VARCHAR:
			case SQLTokenizer.VARBINARY:
			case SQLTokenizer.BINARY:
			case SQLTokenizer.CHAR:
			case SQLTokenizer.NUMERIC:
			case SQLTokenizer.DECIMAL:
				return datatype.getPrecision();
			default:
				return super.getPrecision();
		}
	}
	final int getScale() {
		return datatype.getScale();
	}
}
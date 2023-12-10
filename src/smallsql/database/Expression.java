package smallsql.database;
import java.sql.SQLException;
abstract class Expression implements Cloneable{
	static final Expression NULL = new ExpressionValue( null, SQLTokenizer.NULL );
	final private int type;
	private String name; 
	private String alias;
	private Expression[] params;
	Expression(int type){
		this.type = type;
	}
	protected Object clone() throws CloneNotSupportedException{
		return super.clone();
	}
	final String getName(){ 
		return name; 
	}
	final void setName(String name){ 
		this.alias = this.name = name; 
	}
	final String getAlias(){ 
		return alias; 
	}
	final void setAlias(String alias){ 
		this.alias = alias; 
	}
    void setParams( Expression[] params ){
        this.params = params;
    }
    void setParamAt( Expression param, int idx){
    	params[idx] = param;
    }
    final Expression[] getParams(){ return params; }
    void optimize() throws SQLException{
        if(params != null){
            for(int p=0; p<params.length; p++){
                params[p].optimize();
            }
        }
    }
	public boolean equals(Object expr){
		if(!(expr instanceof Expression)) return false;
		if( ((Expression)expr).type == type){
			Expression[] p1 = ((Expression)expr).params;
			Expression[] p2 = params;
			if(p1 != null && p2 != null){
				if(p1 == null) return false;
				for(int i=0; i<p1.length; i++){
					if(!p2[i].equals(p1[i])) return false;
				}
			}
			String name1 = ((Expression)expr).name;
			String name2 = name;
			if(name1 == name2) return true;
			if(name1 == null) return false;
			if(name1.equalsIgnoreCase(name2)) return true;
		}
		return false;
	}
    abstract boolean isNull() throws Exception;
    abstract boolean getBoolean() throws Exception;
    abstract int getInt() throws Exception;
    abstract long getLong() throws Exception;
    abstract float getFloat() throws Exception;
    abstract double getDouble() throws Exception;
    abstract long getMoney() throws Exception;
    abstract MutableNumeric getNumeric() throws Exception;
    abstract Object getObject() throws Exception;
	final Object getApiObject() throws Exception{
		Object obj = getObject();
		if(obj instanceof Mutable){
			return ((Mutable)obj).getImmutableObject();
		}
		return obj;
	}
    abstract String getString() throws Exception;
    abstract byte[] getBytes() throws Exception;
    abstract int getDataType();
    final int getType(){return type;}
	String getTableName(){
		return null;
	}
	int getPrecision(){
		return SSResultSetMetaData.getDataTypePrecision( getDataType(), -1 );
	}
	int getScale(){
		return getScale(getDataType());
	}
	final static int getScale(int dataType){
		switch(dataType){
			case SQLTokenizer.MONEY:
			case SQLTokenizer.SMALLMONEY:
				return 4;
			case SQLTokenizer.TIMESTAMP:
				return 9; 
			case SQLTokenizer.NUMERIC:
			case SQLTokenizer.DECIMAL:
				return 38;
			default: return 0;
		}
	}
	int getDisplaySize(){
		return SSResultSetMetaData.getDisplaySize(getDataType(), getPrecision(), getScale());
	}
	boolean isDefinitelyWritable(){
		return false;
	}
	boolean isAutoIncrement(){
		return false;
	}
	boolean isCaseSensitive(){
		return false; 
	}
	boolean isNullable(){
		return true; 
	}
    static final int VALUE      = 1;
    static final int NAME       = 2;
    static final int FUNCTION   = 3;
	static final int GROUP_BY   = 11;
	static final int COUNT	    = 12;
	static final int SUM	    = 13;
	static final int FIRST		= 14;
	static final int LAST		= 15;
	static final int MIN		= 16;
	static final int MAX		= 17;
	static final int GROUP_BEGIN= GROUP_BY;
}
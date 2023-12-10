package smallsql.database;
import java.io.*;
import java.nio.channels.FileChannel;
import java.sql.*;
import smallsql.database.language.Language;
class Column implements Cloneable{
    private Expression defaultValue = Expression.NULL; 
    private String defaultDefinition; 
    private String name;
    private boolean identity;
    private boolean caseSensitive;
    private boolean nullable = true;
    private int scale;
    private int precision;
    private int dataType;
    private Identity counter; 
    void setName( String name ){
        this.name = name;
    }
    void setDefaultValue(Expression defaultValue, String defaultDefinition){
        this.defaultValue 		= defaultValue;
        this.defaultDefinition	= defaultDefinition;
    }
    Expression getDefaultValue(SSConnection con) throws SQLException{
    	if(identity)
    		counter.createNextValue(con);
        return defaultValue;
    }
	String getDefaultDefinition(){
		return defaultDefinition;
	}
    String getName(){
        return name;
    }
    boolean isAutoIncrement(){
        return identity;
    }
    void setAutoIncrement(boolean identity){
        this.identity = identity;
    }
    int initAutoIncrement(FileChannel raFile, long filePos) throws IOException{
    	if(identity){
			counter = new Identity(raFile, filePos);
			defaultValue = new ExpressionValue( counter, SQLTokenizer.BIGINT );
    	}
    	return 8;
    }
    void setNewAutoIncrementValue(Expression obj) throws Exception{
		if(identity){
			counter.setNextValue(obj);
		}
    }
    boolean isCaseSensitive(){
        return caseSensitive;
    }
    void setNullable(boolean nullable){
        this.nullable = nullable;
    }
    boolean isNullable(){
        return nullable;
    }
    void setDataType(int dataType){
        this.dataType = dataType;
    }
    int getDataType(){
        return dataType;
    }
    int getDisplaySize(){
		return SSResultSetMetaData.getDisplaySize( dataType, precision, scale);
    }
    void setScale(int scale){
        this.scale = scale;
    }
    int getScale(){
		switch(dataType){
			case SQLTokenizer.DECIMAL:
			case SQLTokenizer.NUMERIC:
				return scale;
			default:
				return Expression.getScale(dataType);
		}
    }
    void setPrecision(int precision) throws SQLException{
	    if(precision<0) throw SmallSQLException.create(Language.COL_INVALID_SIZE, new Object[] { new Integer(precision), name});
        this.precision = precision;
    }
    int getPrecision(){
		return SSResultSetMetaData.getDataTypePrecision( dataType, precision );
    }
    int getColumnSize(){
    	if(SSResultSetMetaData.isNumberDataType(dataType))
    		 return getPrecision();
    	else return getDisplaySize();
    }
    int getFlag(){
        return (identity        ? 1 : 0) |
               (caseSensitive   ? 2 : 0) |
               (nullable        ? 4 : 0);
    }
    void setFlag(int flag){
        identity        = (flag & 1) > 0;
        caseSensitive   = (flag & 2) > 0;
        nullable        = (flag & 4) > 0;
    }
    Column copy(){
    	try{
    		return (Column)clone();
    	}catch(Exception e){return null;}
    }
}
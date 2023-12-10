package smallsql.database;
import java.sql.*;
abstract class Store {
	static final Store NULL = new StoreNull();
	static final Store NOROW= new StoreNoCurrentRow();
	abstract boolean isNull(int offset) throws Exception;
	abstract boolean getBoolean( int offset, int dataType) throws Exception;
	abstract byte[] getBytes( int offset, int dataType) throws Exception;
	abstract double getDouble( int offset, int dataType) throws Exception;
	abstract float getFloat( int offset, int dataType) throws Exception;
	abstract int getInt( int offset, int dataType) throws Exception;
	abstract long getLong( int offset, int dataType) throws Exception;
	abstract long getMoney( int offset, int dataType) throws Exception;
	abstract MutableNumeric getNumeric( int offset, int dataType) throws Exception;
	abstract Object getObject( int offset, int dataType) throws Exception;
	abstract String getString( int offset, int dataType) throws Exception;
	boolean isValidPage(){
		return false;
	}
	abstract void scanObjectOffsets( int[] offsets, int dataTypes[] );
	abstract int getUsedSize();
	abstract long getNextPagePos();
	abstract void deleteRow(SSConnection con) throws SQLException;
}
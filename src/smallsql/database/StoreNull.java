package smallsql.database;
import java.sql.*;
import smallsql.database.language.Language;
class StoreNull extends Store {
	private final long nextPagePos;
	StoreNull(){
		this(-1);
	}
	StoreNull(long nextPos){
		nextPagePos = nextPos;
	}
	final boolean isNull(int offset) {
		return true;
	}
	final boolean getBoolean(int offset, int dataType) throws Exception {
		return false;
	}
	final byte[] getBytes(int offset, int dataType) throws Exception {
		return null;
	}
	final double getDouble(int offset, int dataType) throws Exception {
		return 0;
	}
	final float getFloat(int offset, int dataType) throws Exception {
		return 0;
	}
	final int getInt(int offset, int dataType) throws Exception {
		return 0;
	}
	final long getLong(int offset, int dataType) throws Exception {
		return 0;
	}
	final long getMoney(int offset, int dataType) throws Exception {
		return 0;
	}
	final MutableNumeric getNumeric(int offset, int dataType) throws Exception {
		return null;
	}
	final Object getObject(int offset, int dataType) throws Exception {
		return null;
	}
	final String getString(int offset, int dataType) throws Exception {
		return null;
	}
	final void scanObjectOffsets(int[] offsets, int[] dataTypes) {}
	final int getUsedSize() {
		return 0;
	}
	final long getNextPagePos(){
		return nextPagePos;
	}
	final void deleteRow(SSConnection con) throws SQLException{
		if(nextPagePos >= 0){
			throw SmallSQLException.create(Language.ROW_DELETED);
		}
		throw new Error();
	}
}
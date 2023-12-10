package smallsql.database;
import java.sql.*;
import smallsql.database.language.Language;
public class StoreNoCurrentRow extends Store {
	private SQLException noCurrentRow(){
		return SmallSQLException.create(Language.ROW_NOCURRENT);
	}
	boolean isNull(int offset) throws SQLException {
		throw noCurrentRow();
	}
	boolean getBoolean(int offset, int dataType) throws Exception {
		throw noCurrentRow();
	}
	byte[] getBytes(int offset, int dataType) throws Exception {
		throw noCurrentRow();
	}
	double getDouble(int offset, int dataType) throws Exception {
		throw noCurrentRow();
	}
	float getFloat(int offset, int dataType) throws Exception {
		throw noCurrentRow();
	}
	int getInt(int offset, int dataType) throws Exception {
		throw noCurrentRow();
	}
	long getLong(int offset, int dataType) throws Exception {
		throw noCurrentRow();
	}
	long getMoney(int offset, int dataType) throws Exception {
		throw noCurrentRow();
	}
	MutableNumeric getNumeric(int offset, int dataType) throws Exception {
		throw noCurrentRow();
	}
	Object getObject(int offset, int dataType) throws Exception {
		throw noCurrentRow();
	}
	String getString(int offset, int dataType) throws Exception {
		throw noCurrentRow();
	}
	void scanObjectOffsets(int[] offsets, int[] dataTypes) {
	}
	int getUsedSize() {
		return 0;
	}
	long getNextPagePos(){
		return -1;
	}
	void deleteRow(SSConnection con) throws SQLException{
		throw noCurrentRow();
	}
}
package smallsql.database;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.sql.*;
import smallsql.database.language.Language;
abstract class TableView {
	static final int MAGIC_TABLE = 'S' << 24 | 'Q' << 16 | 'L' << 8 | 'T';
	static final int MAGIC_VIEW  = 'S' << 24 | 'Q' << 16 | 'L' << 8 | 'V';
	static final int TABLE_VIEW_VERSION = 2;
	static final int TABLE_VIEW_OLD_VERSION = 1;
	final String name;
	final Columns columns;
	private long timestamp = System.currentTimeMillis();
	static final int LOCK_NONE   = 0; 
	static final int LOCK_INSERT = 1; 
	static final int LOCK_READ   = 2; 
	static final int LOCK_WRITE  = 3; 
	static final int LOCK_TAB    = 4; 
	TableView(String name, Columns columns){
		this.name = name;
		this.columns = columns;
	}
	static TableView load(SSConnection con, Database database, String name) throws SQLException{
	    FileChannel raFile = null;
		try{
			String fileName = Utils.createTableViewFileName( database, name );
			File file = new File( fileName );
			if(!file.exists())
				throw SmallSQLException.create(Language.TABLE_OR_VIEW_MISSING, name);
			raFile = Utils.openRaFile( file, database.isReadOnly() );
			ByteBuffer buffer = ByteBuffer.allocate(8);
			raFile.read(buffer);
			buffer.position(0);
			int magic   = buffer.getInt();
			int version = buffer.getInt();
			switch(magic){
				case MAGIC_TABLE:
				case MAGIC_VIEW:
						break;
				default:
					throw SmallSQLException.create(Language.TABLE_OR_VIEW_FILE_INVALID, fileName);
			}
			if(version > TABLE_VIEW_VERSION)
				throw SmallSQLException.create(Language.FILE_TOONEW, new Object[] { new Integer(version), fileName });
			if(version < TABLE_VIEW_OLD_VERSION)
				throw SmallSQLException.create(Language.FILE_TOOOLD, new Object[] { new Integer(version), fileName });
			if(magic == MAGIC_TABLE)
				return new Table( database, con, name, raFile, raFile.position(), version);
				return new View ( con, name, raFile, raFile.position());
		}catch(Throwable e){
			if(raFile != null)
				try{
					raFile.close();
				}catch(Exception e2){
					DriverManager.println(e2.toString());
				}
			throw SmallSQLException.createFromException(e);
		}
	}
	File getFile(Database database){
		return new File( Utils.createTableViewFileName( database, name ) );
	}
	FileChannel createFile(SSConnection con, Database database) throws Exception{
	    if( database.isReadOnly() ){
	        throw SmallSQLException.create(Language.DB_READONLY);
	    }
		File file = getFile( database );
		boolean ok = file.createNewFile();
		if(!ok) throw SmallSQLException.create(Language.TABLE_EXISTENT, name);
		FileChannel raFile = Utils.openRaFile( file, database.isReadOnly() );
		con.add(new CreateFile(file, raFile, con, database));
		writeMagic(raFile);
		return raFile;
	}
	abstract void writeMagic(FileChannel raFile) throws Exception;
	String getName(){
		return name;
	}
	long getTimestamp(){
		return timestamp;
	}
	final int findColumnIdx(String columnName){
		for(int i=0; i<columns.size(); i++){
			if( columns.get(i).getName().equalsIgnoreCase(columnName) ) return i;
		}
		return -1;
	}
	final Column findColumn(String columnName){
		for(int i=0; i<columns.size(); i++){
			Column column = columns.get(i);
			if( column.getName().equalsIgnoreCase(columnName) ) return column;
		}
		return null;
	}
	void close() throws Exception{}
}
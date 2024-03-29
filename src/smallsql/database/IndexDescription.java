package smallsql.database;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.sql.DriverManager;
import java.sql.SQLException;
import smallsql.database.language.Language;
final class IndexDescription {
	static final int MAGIC_INDEX = 'S' << 24 | 'Q' << 16 | 'L' << 8 | 'I';
	static final int INDEX_VERSION = 1;
	private final String name;
	final private int constraintType; 
	final private Strings columns;
	private int[] matrix;
	final private Expressions expressions;
	private Index index;
    private FileChannel raFile;
	IndexDescription( String name, String tableName, int constraintType, Expressions expressions, Strings columns){
		this.constraintType = constraintType;
		this.expressions = expressions;
		this.columns = columns;
        this.name = createName(name, tableName);
	}
    private static String createName( String defaultName, String tableName ){
        if(defaultName == null){
            defaultName = tableName + "_" + Long.toHexString(System.currentTimeMillis()) + Integer.toHexString(new Object().hashCode());
        }
        return defaultName;
    }
	final String getName(){
		return name;
	}
	final boolean isPrimary(){
		return constraintType == SQLTokenizer.PRIMARY;
	}
	final boolean isUnique(){
		return constraintType == SQLTokenizer.PRIMARY || constraintType == SQLTokenizer.UNIQUE;
	}
	final Strings getColumns(){
		return columns;
	}
	final int matchFactor(Strings strings){
		if(strings.size() < columns.size())
			return Integer.MAX_VALUE; 
		nextColumn:
		for(int c=0; c<columns.size(); c++){
			String colName = columns.get(c);
			for(int s=0; s<strings.size(); s++){
				if(colName.equalsIgnoreCase(strings.get(s)) )
					continue nextColumn;
			}
			return Integer.MAX_VALUE; 
		}
		return strings.size() - columns.size();
	}
	final void init(Database database, TableView tableView){
		int size = tableView.columns.size();
		matrix = new int[size];
		for(int i=0; i<matrix.length; i++){
			matrix[i] = -1;
		}
		for(int i=0; i<columns.size(); i++){
			matrix[tableView.findColumnIdx(columns.get(i))] = i;
		}
	}
	final void create(SSConnection con, Database database, TableView tableView) throws Exception{
		init( database, tableView );
		raFile = createFile( con, database );
	}
	static File getFile(Database database, String name) throws Exception{
		return new File( Utils.createIdxFileName( database, name ) );
	}
	private FileChannel createFile(SSConnection con, Database database) throws Exception{
	    if( database.isReadOnly() ){
	        throw SmallSQLException.create(Language.DB_READONLY);
	    }
		File file = getFile( database, name );
		boolean ok = file.createNewFile();
		if(!ok) throw SmallSQLException.create(Language.INDEX_EXISTS, name);
		FileChannel randomFile = Utils.openRaFile( file, database.isReadOnly() );
        con.add(new CreateFile(file, randomFile, con, database));
		writeMagic(randomFile);
		return randomFile;
	}
    private void load(Database database) throws SQLException{
        try{
            File file = getFile( database, name );
            if(!file.exists())
                throw SmallSQLException.create(Language.INDEX_MISSING, name);
            raFile = Utils.openRaFile( file, database.isReadOnly() );
            ByteBuffer buffer = ByteBuffer.allocate(8);
            raFile.read(buffer);
            buffer.position(0);
            int magic   = buffer.getInt();
            int version = buffer.getInt();
            if(magic != MAGIC_INDEX){
                throw SmallSQLException.create(Language.INDEX_FILE_INVALID, file.getName());
            }
            if(version > INDEX_VERSION){
            	Object[] params = { new Integer(version), file.getName() };
                throw SmallSQLException.create(Language.FILE_TOONEW, params);
            }
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
	void drop(Database database) throws Exception {
        close();
		boolean ok = getFile( database, name).delete();
		if(!ok) throw SmallSQLException.create(Language.TABLE_CANT_DROP, name);
	}
    void close() throws Exception{
        if(raFile != null){
            raFile.close();
            raFile = null;
        }
    }
	private final void writeMagic(FileChannel raFile) throws Exception{
	    ByteBuffer buffer = ByteBuffer.allocate(8);
	    buffer.putInt(MAGIC_INDEX);
	    buffer.putInt(INDEX_VERSION);
	    buffer.position(0);
	    raFile.write(buffer);
	}
	final void writeExpression( int columnIdx, Expression valueExpression) {
		int idx = matrix[columnIdx];
		if(idx >= 0) 
			expressions.set(idx, valueExpression);
	}
	final void writeFinish(SSConnection con) {
	}
	final void save(StoreImpl store) throws SQLException{
		store.writeInt(constraintType);
		store.writeInt(columns.size());
		for(int c=0; c<columns.size(); c++){
			store.writeString( columns.get(c) );
		}
		store.writeString(name);
	}
	final static IndexDescription load(Database database, TableView tableView, StoreImpl store) throws SQLException{
		int constraintType = store.readInt();
		int count = store.readInt();
		Strings columns = new Strings();
		Expressions expressions = new Expressions();
		SQLParser sqlParser = new SQLParser();
		for(int c=0; c<count; c++){
			String column = store.readString();
			columns.add( column );
			expressions.add( sqlParser.parseExpression(column));
		}
		IndexDescription indexDesc = new IndexDescription( store.readString(), tableView.name, constraintType, expressions, columns);
        indexDesc.init( database, tableView );
        indexDesc.load(database);
		return indexDesc;
	}
}
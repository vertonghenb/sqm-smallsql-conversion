package smallsql.database;
import java.sql.SQLException;
import smallsql.database.language.Language;
final class CommandTable extends Command{
	final private Columns columns = new Columns();
	final private IndexDescriptions indexes = new IndexDescriptions();
    final private ForeignKeys foreignKeys = new ForeignKeys();
    final private int tableCommandType;
    CommandTable( Logger log, String catalog, String name, int tableCommandType ){
    	super(log);
        this.type = SQLTokenizer.TABLE;
        this.catalog = catalog;
        this.name = name;
        this.tableCommandType = tableCommandType;
    }
    void addColumn(Column column) throws SQLException{
        addColumn(columns, column);
    }
	void addIndex( IndexDescription indexDescription ) throws SQLException{
		indexes.add(indexDescription);
	}
    void addForeingnKey(ForeignKey key){
        foreignKeys.add(key);
    }
    void executeImpl(SSConnection con, SSStatement st) throws Exception{
        Database database = catalog == null ? 
                con.getDatabase(false) : 
                Database.getDatabase( catalog, con, false );
        switch(tableCommandType){
        case SQLTokenizer.CREATE:
            database.createTable( con, name, columns, indexes, foreignKeys );
            break;
        case SQLTokenizer.ADD:
            con = new SSConnection(con);
            Table oldTable = (Table)database.getTableView( con, name);
            TableStorePage tableLock = oldTable.requestLock( con, SQLTokenizer.ALTER, -1);
            String newName = "#" + System.currentTimeMillis() + this.hashCode();
            try{
                Columns oldColumns = oldTable.columns;
                Columns newColumns = oldColumns.copy();
                for(int i = 0; i < columns.size(); i++){
                    addColumn(newColumns, columns.get(i));
                }
                Table newTable = database.createTable( con, newName, newColumns, oldTable.indexes, indexes, foreignKeys );
                StringBuffer buffer = new StringBuffer(256);
                buffer.append("INSERT INTO ").append( newName ).append( '(' );
                for(int c=0; c<oldColumns.size(); c++){
                    if(c != 0){
                        buffer.append( ',' );
                    }
                    buffer.append( oldColumns.get(c).getName() );
                }
                buffer.append( ")  SELECT * FROM " ).append( name );
                con.createStatement().execute( buffer.toString() );
                database.replaceTable( oldTable, newTable );
            }catch(Exception ex){
                try {
                    database.dropTable(con, newName);
                } catch (Exception ex1) {}
                try{
                    indexes.drop(database);
                } catch (Exception ex1) {}
                throw ex;
            }finally{
                tableLock.freeLock();
            }
            break;
        default:
            throw new Error();
        }
    }
    private void addColumn(Columns cols, Column column) throws SQLException{
        if(cols.get(column.getName()) != null){
            throw SmallSQLException.create(Language.COL_DUPLICATE, column.getName());
        }
        cols.add(column);
    }
}
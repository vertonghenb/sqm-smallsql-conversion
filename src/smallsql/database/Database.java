package smallsql.database;
import java.util.*;
import java.io.*;
import java.nio.channels.FileChannel;
import java.sql.*;
import smallsql.database.language.Language;
final class Database{
    static private HashMap databases = new HashMap();
    private final TableViewMap tableViews = new TableViewMap();
    private final String name;
    private final boolean readonly;
	private final File directory;
	private final FileChannel master;
	private final WeakHashMap connections = new WeakHashMap();
    static Database getDatabase(String name, SSConnection con, boolean create) throws SQLException{
        if(name == null){
            return null;
        }
        if(name.startsWith("file:")){
            name = name.substring(5);
        }
        File file;
        try{
            file = new File(name).getCanonicalFile();
        }catch(Throwable th){
            throw SmallSQLException.createFromException( th );
        }
        String dbKey = file.getName() + ";readonly=" + con.isReadOnly();
        synchronized(databases){
            Database db = (Database)databases.get(dbKey);
            if(db == null){
                if(create && !file.isDirectory()){
                    CommandCreateDatabase command = new CommandCreateDatabase(con.log, name);
                    command.execute(con, null);
                }
                db = new Database( name, file, con.isReadOnly() );
                databases.put(dbKey, db);
            }
            db.connections.put(con, null);
            return db;
        }
    }
    private static Database getDatabase(SSConnection con, String name) throws SQLException{
		return name == null ?
					con.getDatabase(false) :
					getDatabase( name, con, false );
    }
    private Database( String name, File canonicalFile, boolean readonly ) throws SQLException{
        try{
	        this.name = name;
	        this.readonly = readonly;
			directory = canonicalFile;
			if(!directory.isDirectory()){
                throw SmallSQLException.create(Language.DB_NONEXISTENT, name);
            }
			File file = new File( directory, Utils.MASTER_FILENAME);
			if(!file.exists())
				throw SmallSQLException.create(Language.DB_NOT_DIRECTORY, name);
			master = Utils.openRaFile( file, readonly );
        }catch(Exception e){
        	throw SmallSQLException.createFromException(e);
        }
    }
    String getName(){
        return name;
    }
	boolean isReadOnly(){
	    return readonly;
	}
	static final void closeConnection(SSConnection con) throws SQLException{
		synchronized(databases){
			Iterator iterator = databases.values().iterator();
			while(iterator.hasNext()){
				Database database = (Database)iterator.next();
				WeakHashMap connections = database.connections;
				connections.remove(con);
				if(connections.size() == 0){
					try {
						iterator.remove();
						database.close();
					} catch (Exception e) {
						throw SmallSQLException.createFromException(e);
					}
				}
			}
		}
	}
	private final void close() throws Exception{
		synchronized(tableViews){
			Iterator iterator = tableViews.values().iterator();
			while(iterator.hasNext()){
				TableView tableView = (TableView)iterator.next();
				tableView.close();
				iterator.remove();
			}
		}
		master.close();
	}
    static TableView getTableView(SSConnection con, String catalog, String tableName) throws SQLException{
    	return getDatabase( con, catalog).getTableView( con, tableName);
    }
    TableView getTableView(SSConnection con, String tableName) throws SQLException{
        synchronized(tableViews){
            TableView tableView = tableViews.get(tableName);
            if(tableView == null){
                tableView = TableView.load(con, this, tableName);
                tableViews.put( tableName, tableView);
            }
            return tableView;
        }
    }
	static void dropTable(SSConnection con, String catalog, String tableName) throws Exception{
		getDatabase( con, catalog).dropTable( con, tableName);
	}
    void dropTable(SSConnection con, String tableName) throws Exception{
        synchronized(tableViews){
            Table table = (Table)tableViews.get( tableName );
            if(table != null){
				tableViews.remove( tableName );
                table.drop(con);
            }else{
            	Table.drop( this, tableName );
            }
        }
    }
    void removeTableView(String tableViewName){
        synchronized(tableViews){
            tableViews.remove( tableViewName );
        }
    }
    void replaceTable( Table oldTable, Table newTable) throws Exception{
        synchronized(tableViews){
            tableViews.remove( oldTable.name );
            tableViews.remove( newTable.name );
            oldTable.close();
            newTable.close();
            File oldFile = oldTable.getFile(this);
            File newFile = newTable.getFile(this);
            File tmpFile = new File(Utils.createTableViewFileName( this, "#" + System.currentTimeMillis() + this.hashCode() ));
            if( !oldFile.renameTo(tmpFile) ){
                throw SmallSQLException.create(Language.TABLE_CANT_RENAME, oldTable.name);
            }
            if( !newFile.renameTo(oldFile) ){
                tmpFile.renameTo(oldFile); 
                throw SmallSQLException.create(Language.TABLE_CANT_RENAME, oldTable.name);
            }
            tmpFile.delete();
        }
    }
	static void dropView(SSConnection con, String catalog, String tableName) throws Exception{
		getDatabase( con, catalog).dropView(tableName);
	}
	void dropView(String viewName) throws Exception{
		synchronized(tableViews){
			Object view = tableViews.remove( viewName );
			if(view != null && !(view instanceof View))
				throw SmallSQLException.create(Language.VIEWDROP_NOT_VIEW, viewName);
			View.drop( this, viewName );
		}
	}
    private void checkForeignKeys( SSConnection con, ForeignKeys foreignKeys ) throws SQLException{
        for(int i=0; i<foreignKeys.size(); i++){
            ForeignKey foreignKey = foreignKeys.get(i);
            TableView pkTable = getTableView(con, foreignKey.pkTable);
            if(!(pkTable instanceof Table)){
                throw SmallSQLException.create(Language.FK_NOT_TABLE, foreignKey.pkTable);
            }
        }
    }
	void createTable(SSConnection con, String name, Columns columns, IndexDescriptions indexes, ForeignKeys foreignKeys) throws Exception{
        checkForeignKeys( con, foreignKeys );
        Table table = new Table( this, con, name, columns, indexes, foreignKeys);
        synchronized(tableViews){
            tableViews.put( name, table);
        }
    }
    Table createTable(SSConnection con, String tableName, Columns columns, IndexDescriptions oldIndexes, IndexDescriptions newIndexes, ForeignKeys foreignKeys) throws Exception{
        checkForeignKeys( con, foreignKeys );
        Table table = new Table( this, con, tableName, columns, oldIndexes, newIndexes, foreignKeys);
        synchronized(tableViews){
            tableViews.put( tableName, table);
        }
        return table;
    }
	void createView(SSConnection con, String viewName, String sql) throws Exception{
		new View( this, con, viewName, sql);
	}
    static Object[][] getCatalogs(Database database){
    	List catalogs = new ArrayList();
    	File baseDir = (database != null) ?
    					database.directory.getParentFile() :
						new File(".");
		File dirs[] = baseDir.listFiles();
		if(dirs != null)
			for(int i=0; i<dirs.length; i++){
				if(dirs[i].isDirectory()){
					if(new File(dirs[i], Utils.MASTER_FILENAME).exists()){
						Object[] catalog = new Object[1];
						catalog[0] = dirs[i].getPath();
						catalogs.add(catalog);
					}
				}
			}
		Object[][] result = new Object[catalogs.size()][];
		catalogs.toArray(result);
		return result;
    }
	Strings getTables(String tablePattern){
		Strings list = new Strings();
		File dirs[] = directory.listFiles();    
		if(dirs != null)
			if(tablePattern == null) tablePattern = "%"; 
			tablePattern += Utils.TABLE_VIEW_EXTENTION;
			for(int i=0; i<dirs.length; i++){
				String name = dirs[i].getName();
				if(Utils.like(name, tablePattern)){
					list.add(name.substring( 0, name.length()-Utils.TABLE_VIEW_EXTENTION.length() ));
				}
			}
    	return list;
    }
    Object[][] getColumns( SSConnection con, String tablePattern, String colPattern) throws Exception{
    	List rows = new ArrayList();
		Strings tables = getTables(tablePattern);
    	for(int i=0; i<tables.size(); i++){
    		String tableName = tables.get(i);
			try{
	    		TableView tab = getTableView( con, tableName);
	    		Columns cols = tab.columns;
	    		for(int c=0; c<cols.size(); c++){
	    			Column col = cols.get(c);
					Object[] row = new Object[18];
					row[0] = getName(); 			
					row[2] = tableName;				
					row[3] = col.getName();			
					row[4] = Utils.getShort( SQLTokenizer.getSQLDataType( col.getDataType() )); 
					row[5] = SQLTokenizer.getKeyWord( col.getDataType() );	
					row[6] = Utils.getInteger(col.getColumnSize());
					row[8] = Utils.getInteger(col.getScale());
					row[9] = Utils.getInteger(10);		
					row[10]= Utils.getInteger(col.isNullable() ? DatabaseMetaData.columnNullable : DatabaseMetaData.columnNoNulls); 
					row[12]= col.getDefaultDefinition(); 
					row[15]= row[6];				
					row[16]= Utils.getInteger(i); 	
					row[17]= col.isNullable() ? "YES" : "NO"; 
					rows.add(row);
	    		}
			}catch(Exception e){
			}
    	}
		Object[][] result = new Object[rows.size()][];
		rows.toArray(result);
		return result;
    }
	Object[][] getReferenceKeys(SSConnection con, String pkTable, String fkTable) throws SQLException{
		List rows = new ArrayList();
		Strings tables = (pkTable != null) ? getTables(pkTable) : getTables(fkTable);
		for(int t=0; t<tables.size(); t++){
    		String tableName = tables.get(t);
    		TableView tab = getTableView( con, tableName);
			if(!(tab instanceof Table)) continue;
			ForeignKeys references = ((Table)tab).references;
			for(int i=0; i<references.size(); i++){
				ForeignKey foreignKey = references.get(i);
				IndexDescription pk = foreignKey.pk;
				IndexDescription fk = foreignKey.fk;
				if((pkTable == null || pkTable.equals(foreignKey.pkTable)) &&
				   (fkTable == null || fkTable.equals(foreignKey.fkTable))){
					Strings columnsPk = pk.getColumns();
					Strings columnsFk = fk.getColumns();
					for(int c=0; c<columnsPk.size(); c++){
						Object[] row = new Object[14];
						row[0] = getName();				
						row[2] = foreignKey.pkTable;	
						row[3] = columnsPk.get(c);		
						row[4] = getName();				
						row[6] = foreignKey.fkTable;	
						row[7] = columnsFk.get(c);		
						row[8] = Utils.getShort(c+1);	
						row[9] = Utils.getShort(foreignKey.updateRule);
						row[10]= Utils.getShort(foreignKey.deleteRule); 
						row[11]= fk.getName();	
						row[12]= pk.getName();	
						row[13]= Utils.getShort(DatabaseMetaData.importedKeyNotDeferrable); 
						rows.add(row);
					}
				}
			}
		}
		Object[][] result = new Object[rows.size()][];
		rows.toArray(result);
		return result;		
	}
	Object[][] getBestRowIdentifier(SSConnection con, String table) throws SQLException{
		List rows = new ArrayList();
		Strings tables = getTables(table);
		for(int t=0; t<tables.size(); t++){
    		String tableName = tables.get(t);
    		TableView tab = getTableView( con, tableName);
			if(!(tab instanceof Table)) continue;
			IndexDescriptions indexes = ((Table)tab).indexes;
			for(int i=0; i<indexes.size(); i++){
				IndexDescription index = indexes.get(i);
				if(index.isUnique()){
					Strings columns = index.getColumns();
					for(int c=0; c<columns.size(); c++){
						String columnName = columns.get(c);
						Column column = tab.findColumn(columnName);
						Object[] row = new Object[8];
						row[0] = Utils.getShort(DatabaseMetaData.bestRowSession);
						row[1] = columnName;			
						final int dataType = column.getDataType();
						row[2] = Utils.getInteger(dataType);
						row[3] = SQLTokenizer.getKeyWord(dataType);
						row[4] = Utils.getInteger(column.getPrecision());	
						row[6] = Utils.getShort(column.getScale());		
						row[7] = Utils.getShort(DatabaseMetaData.bestRowNotPseudo);
						rows.add(row);
					}
				}
			}
		}
		Object[][] result = new Object[rows.size()][];
		rows.toArray(result);
		return result;		
	}
	Object[][] getPrimaryKeys(SSConnection con, String table) throws SQLException{
		List rows = new ArrayList();
		Strings tables = getTables(table);
		for(int t=0; t<tables.size(); t++){
    		String tableName = tables.get(t);
    		TableView tab = getTableView( con, tableName);
			if(!(tab instanceof Table)) continue;
			IndexDescriptions indexes = ((Table)tab).indexes;
			for(int i=0; i<indexes.size(); i++){
				IndexDescription index = indexes.get(i);
				if(index.isPrimary()){
					Strings columns = index.getColumns();
					for(int c=0; c<columns.size(); c++){
						Object[] row = new Object[6];
						row[0] = getName(); 			
						row[2] = tableName;				
						row[3] = columns.get(c);		
						row[4] = Utils.getShort(c+1);	
						row[5] = index.getName();		
						rows.add(row);
					}
				}
			}
		}
		Object[][] result = new Object[rows.size()][];
		rows.toArray(result);
		return result;		
	}
	Object[][] getIndexInfo( SSConnection con, String table, boolean unique) throws SQLException {
		List rows = new ArrayList();
		Strings tables = getTables(table);
		Short type = Utils.getShort( DatabaseMetaData.tableIndexOther );
		for(int t=0; t<tables.size(); t++){
    		String tableName = tables.get(t);
    		TableView tab = getTableView( con, tableName);
			if(!(tab instanceof Table)) continue;
			IndexDescriptions indexes = ((Table)tab).indexes;
			for(int i=0; i<indexes.size(); i++){
				IndexDescription index = indexes.get(i);
				Strings columns = index.getColumns();
				for(int c=0; c<columns.size(); c++){
					Object[] row = new Object[13];
					row[0] = getName(); 			
					row[2] = tableName;				
					row[3] = Boolean.valueOf(!index.isUnique());
					row[5] = index.getName();		
					row[6] = type;					
					row[7] = Utils.getShort(c+1);	
					row[8] = columns.get(c);		
					rows.add(row);
				}
			}
    	}
		Object[][] result = new Object[rows.size()][];
		rows.toArray(result);
		return result;
	}
}
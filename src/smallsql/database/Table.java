package smallsql.database;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import smallsql.database.language.Language;
class Table extends TableView{
	private static final int INDEX = 1;
    final Database database;
    FileChannel raFile; 
	private Lobs lobs; 
    long firstPage; 
	final private HashMap locks = new HashMap();
	private SSConnection tabLockConnection; 
	private int tabLockCount;
	final private ArrayList locksInsert = new ArrayList(); 
	final private HashMap serializeConnections = new HashMap();
	final IndexDescriptions indexes;
	final ForeignKeys references;
    Table( Database database, SSConnection con, String name, FileChannel raFile, long offset, int tableFormatVersion) throws Exception{
        super( name, new Columns() );
        this.database = database;
        this.raFile   = raFile;
		this.firstPage = offset;
		StoreImpl store = getStore(con, firstPage, SQLTokenizer.SELECT);
        if(store == null){
            throw SmallSQLException.create(Language.TABLE_FILE_INVALID, getFile(database));
        }
		int count = store.readInt();
		for(int i=0; i<count; i++){
			columns.add( store.readColumn(tableFormatVersion) );
		}
		indexes = new IndexDescriptions();
        references = new ForeignKeys();
		int type;
		while((type = store.readInt()) != 0){
			int offsetInPage = store.getCurrentOffsetInPage();
			int size = store.readInt();
			switch(type){
				case INDEX:
					indexes.add( IndexDescription.load( database, this, store) );
					break;
			}
			store.setCurrentOffsetInPage(offsetInPage + size);
		}
		firstPage = store.getNextPagePos();
    }
    Table(Database database, SSConnection con, String name, Columns columns, IndexDescriptions indexes, ForeignKeys foreignKeys) throws Exception{
        this(database, con, name, columns, null, indexes, foreignKeys);
    }
    Table(Database database, SSConnection con, String name, Columns columns, IndexDescriptions existIndexes, IndexDescriptions newIndexes, ForeignKeys foreignKeys) throws Exception{
        super( name, columns );
        this.database = database;
        this.references = foreignKeys;
        newIndexes.create(con, database, this);
        if(existIndexes == null){
            this.indexes = newIndexes;
        }else{
            this.indexes = existIndexes;
            existIndexes.add(newIndexes);
        }
        write(con);
        for(int i=0; i<foreignKeys.size(); i++){
            ForeignKey foreignKey = foreignKeys.get(i);
            Table pkTable = (Table)database.getTableView(con, foreignKey.pkTable);
            pkTable.references.add(foreignKey);
        }
    }
    Table(Database database, String name){
    	super( name, null);
    	this.database = database;
		indexes = null;
        references = null;
    }
    static void drop(Database database, String name) throws Exception{
        boolean ok = new File( Utils.createTableViewFileName( database, name ) ).delete();
        if(!ok) throw SmallSQLException.create(Language.TABLE_CANT_DROP, name);
    }
    void drop(SSConnection con) throws Exception{
		TableStorePage storePage = requestLock( con, SQLTokenizer.CREATE, -1 );
		if(storePage == null){
			throw SmallSQLException.create(Language.TABLE_CANT_DROP_LOCKED, name);
        }
		con.rollbackFile(raFile);
		close();
		if(lobs != null)
			lobs.drop(con);
		if(indexes != null)
			indexes.drop(database);
		boolean ok = getFile(database).delete();
		if(!ok) throw SmallSQLException.create(Language.TABLE_CANT_DROP, name);
    }
    @Override
    void close() throws Exception{
        if(indexes != null)
            indexes.close();
        raFile.close();
        raFile = null;
        if( lobs != null ){
            lobs.close();
            lobs = null;
        }
    }
    private void write(SSConnection con) throws Exception{
        raFile = createFile( con, database );
        firstPage = 8;
        StoreImpl store = getStore( con, firstPage, SQLTokenizer.CREATE);
        int count = columns.size();
        store.writeInt( count );
        for(int i=0; i<count; i++){
            store.writeColumn(columns.get(i));
        }
		for(int i=0; i<indexes.size(); i++){
			IndexDescription indexDesc = indexes.get(i);
			store.writeInt( INDEX );
			int offsetStart = store.getCurrentOffsetInPage();
			store.setCurrentOffsetInPage( offsetStart + 4 ); 
			indexDesc.save(store);
			int offsetEnd = store.getCurrentOffsetInPage();
			store.setCurrentOffsetInPage( offsetStart );
			store.writeInt( offsetEnd - offsetStart);
			store.setCurrentOffsetInPage( offsetEnd );
		}
		store.writeInt( 0 ); 
		store.writeFinsh(null); 
        firstPage = store.getNextPagePos();
    }
	@Override
    void writeMagic(FileChannel raFile) throws Exception{
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putInt(MAGIC_TABLE);
        buffer.putInt(TABLE_VIEW_VERSION);
        buffer.position(0);
        raFile.write(buffer);
	}
    StoreImpl getStore( SSConnection con, long filePos, int pageOperation ) throws Exception{
		TableStorePage storePage = requestLock( con, pageOperation, filePos );
        return StoreImpl.createStore( this, storePage, pageOperation, filePos );
    }
	StoreImpl getStore( TableStorePage storePage, int pageOperation ) throws Exception{
		return StoreImpl.recreateStore( this, storePage, pageOperation );
	}
    StoreImpl getStoreInsert( SSConnection con ) throws Exception{
		TableStorePage storePage = requestLock( con, SQLTokenizer.INSERT, -1 );
        return StoreImpl.createStore( this, storePage, SQLTokenizer.INSERT, -1 );
    }
	StoreImpl getStoreTemp( SSConnection con ) throws Exception{
		TableStorePage storePage = new TableStorePage( con, this, LOCK_NONE, -2);
		return StoreImpl.createStore( this, storePage, SQLTokenizer.INSERT, -2 );
	}
	StoreImpl getLobStore(SSConnection con, long filePos, int pageOperation) throws Exception{
		if(lobs == null){
			lobs = new Lobs( this );
		}
		return lobs.getStore( con, filePos, pageOperation );
	}
    final long getFirstPage(){
        return firstPage;
    }
    List getInserts(SSConnection con){
		synchronized(locks){
			ArrayList inserts = new ArrayList();
			if(con.isolationLevel <= Connection.TRANSACTION_READ_UNCOMMITTED){
				for(int i=0; i<locksInsert.size(); i++){
					TableStorePageInsert lock = (TableStorePageInsert)locksInsert.get(i);
					inserts.add(lock.getLink());
				}
			}else{
				for(int i=0; i<locksInsert.size(); i++){
					TableStorePageInsert lock = (TableStorePageInsert)locksInsert.get(i);
					if(lock.con == con)
						inserts.add(lock.getLink());
				}
			}
			return inserts;
		}    	
    }
    final TableStorePage requestLock(SSConnection con, int pageOperation, long page) throws Exception{
    	synchronized(locks){
            if(raFile == null){
                throw SmallSQLException.create(Language.TABLE_MODIFIED, name);
            }
			long endTime = 0;
			while(true){
				TableStorePage storePage = requestLockImpl( con, pageOperation, page);
				if(storePage != null) 
					return storePage; 
				if(endTime == 0)
					endTime = System.currentTimeMillis() + 5000;
				long waitTime = endTime - System.currentTimeMillis();
				if(waitTime <= 0)
					throw SmallSQLException.create(Language.TABLE_DEADLOCK, name);
				locks.wait(waitTime);
			}
    	}
    }
	final private TableStorePage requestLockImpl(SSConnection con, int pageOperation, long page) throws SQLException{
		synchronized(locks){
			if(tabLockConnection != null && tabLockConnection != con) return null;
			switch(con.isolationLevel){
				case Connection.TRANSACTION_SERIALIZABLE:
					serializeConnections.put( con, con);
					break;
			}
			switch(pageOperation){
				case SQLTokenizer.CREATE:{
						if(locks.size() > 0){
							Iterator values = locks.values().iterator();
							while(values.hasNext()){
								TableStorePage lock = (TableStorePage)values.next();
								if(lock.con != con) return null;
							}
						}
						for(int i=0; i<locksInsert.size(); i++){
							TableStorePageInsert lock = (TableStorePageInsert)locksInsert.get(i);
							if(lock.con != con) return null;
						}
						if(serializeConnections.size() > 0){
							Iterator values = locks.values().iterator();
							while(values.hasNext()){
								TableStorePage lock = (TableStorePage)values.next();
								if(lock.con != con) return null;
							}
						}
						tabLockConnection = con;
						tabLockCount++;
						TableStorePage lock = new TableStorePage(con, this, LOCK_TAB, page);
						con.add(lock);
						return lock;
					}
                case SQLTokenizer.ALTER:{
                    if(locks.size() > 0 || locksInsert.size() > 0){
                        return null;
                    }
                    if(serializeConnections.size() > 0){
                        Iterator values = locks.values().iterator();
                        while(values.hasNext()){
                            TableStorePage lock = (TableStorePage)values.next();
                            if(lock.con != con) return null;
                        }
                    }
                    tabLockConnection = con;
                    tabLockCount++;
                    TableStorePage lock = new TableStorePage(con, this, LOCK_TAB, page);
                    lock.rollback();
                    return lock;
                }
				case SQLTokenizer.INSERT:{
						if(serializeConnections.size() > 1) return null;
						if(serializeConnections.size() == 1 && serializeConnections.get(con) == null) return null;
						TableStorePageInsert lock = new TableStorePageInsert(con, this, LOCK_INSERT);
						locksInsert.add( lock );
						con.add(lock);
						return lock;
					}
				case SQLTokenizer.SELECT:
				case SQLTokenizer.UPDATE:{
						Long pageKey = new Long(page); 
						TableStorePage prevLock = null;
						TableStorePage lock = (TableStorePage)locks.get( pageKey );
						TableStorePage usableLock = null;
						while(lock != null){
							if(lock.con == con || 
							   con.isolationLevel <= Connection.TRANSACTION_READ_UNCOMMITTED){
							    usableLock = lock;
							} else {
							    if(lock.lockType == LOCK_WRITE){
							        return null; 
							    }
							}
							prevLock = lock;
							lock = lock.nextLock;
						}
						if(usableLock != null){
						    return usableLock;
						}
						lock = new TableStorePage( con, this, LOCK_NONE, page);
						if(con.isolationLevel >= Connection.TRANSACTION_REPEATABLE_READ || pageOperation == SQLTokenizer.UPDATE){
							lock.lockType = pageOperation == SQLTokenizer.UPDATE ? LOCK_WRITE : LOCK_READ;
							if(prevLock != null){
							    prevLock.nextLock = lock.nextLock;
							}else{
							    locks.put( pageKey, lock );
							}
							con.add(lock);
						}
						return lock;							
					}
				case SQLTokenizer.LONGVARBINARY:
					return new TableStorePage( con, this, LOCK_INSERT, -1);
				default:
					throw new Error("pageOperation:"+pageOperation);
			}
		}
	}
	TableStorePage requestWriteLock(SSConnection con, TableStorePage readlock) throws SQLException{
		if(readlock.lockType == LOCK_INSERT){
			TableStorePage lock = new TableStorePage( con, this, LOCK_INSERT, -1);
			readlock.nextLock = lock;
			con.add(lock);
			return lock;									
		}
		Long pageKey = new Long(readlock.fileOffset); 
		TableStorePage prevLock = null;
		TableStorePage lock = (TableStorePage)locks.get( pageKey );
		while(lock != null){
			if(lock.con != con) return null; 
			if(lock.lockType < LOCK_WRITE){
				lock.lockType = LOCK_WRITE;
				return lock;
			}
			prevLock = lock;
			lock = lock.nextLock;
		}
		lock = new TableStorePage( con, this, LOCK_WRITE, readlock.fileOffset);
		if(prevLock != null){
		    prevLock.nextLock = lock;
		} else {
		    locks.put( pageKey, lock );
		}
		con.add(lock);
		return lock;									
	}
	void freeLock(TableStorePage storePage){
		final int lockType = storePage.lockType;
		final long fileOffset = storePage.fileOffset;
		synchronized(locks){
			try{
				TableStorePage lock;
				TableStorePage prev;
				switch(lockType){
					case LOCK_INSERT:
						for(int i=0; i<locksInsert.size(); i++){
							prev = lock = (TableStorePage)locksInsert.get(i);
							while(lock != null){
								if(lock == storePage){
									if(lock == prev){
										if(lock.nextLock == null){
											locksInsert.remove(i--);
										}else{
											locksInsert.set( i, lock.nextLock );
										}
									}else{
										prev.nextLock = lock.nextLock;
									}
									return;
								}
								prev = lock;
								lock = lock.nextLock;
							}
						}
						break;
					case LOCK_READ:
					case LOCK_WRITE:
						Long pageKey = new Long(fileOffset); 
						lock = (TableStorePage)locks.get( pageKey );
						prev = lock;
						while(lock != null){
							if(lock == storePage){
								if(lock == prev){
									if(lock.nextLock == null){
										locks.remove(pageKey);
									}else{
										locks.put( pageKey, lock.nextLock );
									}
								}else{
									prev.nextLock = lock.nextLock;
								}
								return;
							}
							prev = lock;
							lock = lock.nextLock;
						}
						break;
					case LOCK_TAB:
						assert storePage.con == tabLockConnection : "Internal Error with TabLock";
						if(--tabLockCount == 0) tabLockConnection = null;
						break;
					default:
						throw new Error();
				}
			}finally{
				locks.notifyAll();
			}
		}
	}
}
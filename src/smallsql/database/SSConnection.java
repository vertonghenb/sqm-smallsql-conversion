package smallsql.database;
import java.nio.channels.FileChannel;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import smallsql.database.language.Language;
public class SSConnection implements Connection {
    private final boolean readonly;
    private Database database;
    private boolean autoCommit = true;
    int isolationLevel = TRANSACTION_READ_COMMITTED; 
    private List commitPages = new ArrayList();
    private long transactionTime;
    private final SSDatabaseMetaData metadata;
    private int holdability;
    final Logger log;
    SSConnection( Properties props ) throws SQLException{
    	SmallSQLException.setLanguage(props.get("locale"));
        log = new Logger();
        String name = props.getProperty("dbpath");
        readonly = "true".equals(props.getProperty("readonly"));
        boolean create = "true".equals(props.getProperty("create"));
        database = Database.getDatabase(name, this, create);
		metadata = new SSDatabaseMetaData(this);
    }
    SSConnection( SSConnection con ){
        readonly = con.readonly;
        database = con.database;
        metadata = con.metadata;
        log      = con.log;
    }
    Database getDatabase(boolean returnNull) throws SQLException{
        testClosedConnection();
    	if(!returnNull && database == null) throw SmallSQLException.create(Language.DB_NOTCONNECTED);
    	return database;
    }
    Object getMonitor(){
        return this;
    }
    public Statement createStatement() throws SQLException {
        return new SSStatement(this);
    }
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return new SSPreparedStatement( this, sql);
    }
    public CallableStatement prepareCall(String sql) throws SQLException {
        return new SSCallableStatement( this, sql);
    }
    public String nativeSQL(String sql){
        return sql;
    }
    public void setAutoCommit(boolean autoCommit) throws SQLException {
		if(log.isLogging()) log.println("AutoCommit:"+autoCommit);
    	if(this.autoCommit != autoCommit){
    		commit();
    		this.autoCommit = autoCommit;
    	}
    }
    public boolean getAutoCommit(){
        return autoCommit;
    }
	void add(TransactionStep storePage) throws SQLException{
		testClosedConnection();
		synchronized(getMonitor()){
            commitPages.add(storePage);
        }
	}
    public void commit() throws SQLException {
        log.println("Commit");
        testClosedConnection();
        synchronized(getMonitor()){
    	try{
	            int count = commitPages.size();
	            for(int i=0; i<count; i++){
	                TransactionStep page = (TransactionStep)commitPages.get(i);
	                page.commit();
	            }
				for(int i=0; i<count; i++){
				    TransactionStep page = (TransactionStep)commitPages.get(i);
					page.freeLock();
				}
	            commitPages.clear();
	            transactionTime = System.currentTimeMillis();
    	}catch(Throwable e){
    		rollback();
    		throw SmallSQLException.createFromException(e);
    	}
        }
    }
	void rollbackFile(FileChannel raFile) throws SQLException{
		testClosedConnection();
		synchronized(getMonitor()){
            for(int i = commitPages.size() - 1; i >= 0; i--){
                TransactionStep page = (TransactionStep)commitPages.get(i);
                if(page.raFile == raFile){
                    page.rollback();
                    page.freeLock();
                }
            }
        }
	}
    void rollback(int savepoint) throws SQLException{
		testClosedConnection();
		synchronized(getMonitor()){
            for(int i = commitPages.size() - 1; i >= savepoint; i--){
                TransactionStep page = (TransactionStep)commitPages.remove(i);
                page.rollback();
                page.freeLock();
            }
        }
    }
    public void rollback() throws SQLException {
		log.println("Rollback");
		testClosedConnection();
        synchronized(getMonitor()){
            int count = commitPages.size();
            for(int i=0; i<count; i++){
                TransactionStep page = (TransactionStep)commitPages.get(i);
                page.rollback();
                page.freeLock();
            }
            commitPages.clear();
			transactionTime = System.currentTimeMillis();
        }
    }
    public void close() throws SQLException {
        rollback();
		database = null;
        commitPages = null;
		Database.closeConnection(this);
    }
	final void testClosedConnection() throws SQLException{
		if(isClosed()) throw SmallSQLException.create(Language.CONNECTION_CLOSED);
	}
    public boolean isClosed(){
        return (commitPages == null);
    }
    public DatabaseMetaData getMetaData(){
        return metadata;
    }
    public void setReadOnly(boolean readOnly){
    }
    public boolean isReadOnly(){
        return readonly;
    }
    public void setCatalog(String catalog) throws SQLException {
        testClosedConnection();
        database = Database.getDatabase(catalog, this, false);
    }
    public String getCatalog(){
    	if(database == null)
    		return "";
        return database.getName();
    }
    public void setTransactionIsolation(int level) throws SQLException {
    	if(!metadata.supportsTransactionIsolationLevel(level)) {
    		throw SmallSQLException.create(Language.ISOLATION_UNKNOWN, String.valueOf(level));
    	}
        isolationLevel = level;        
    }
    public int getTransactionIsolation(){
        return isolationLevel;
    }
    public SQLWarning getWarnings(){
        return null;
    }
    public void clearWarnings(){
    }
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return new SSStatement( this, resultSetType, resultSetConcurrency);
    }
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return new SSPreparedStatement( this, sql, resultSetType, resultSetConcurrency);
    }
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return new SSCallableStatement( this, sql, resultSetType, resultSetConcurrency);
    }
    public Map getTypeMap(){
        return null;
    }
    public void setHoldability(int holdability){
        this.holdability = holdability;
    }
    public int getHoldability(){
        return holdability;
    }
	int getSavepoint() throws SQLException{
		testClosedConnection();
		return commitPages.size(); 
	}
    public Savepoint setSavepoint() throws SQLException {
        return new SSSavepoint(getSavepoint(), null, transactionTime);
    }
    public Savepoint setSavepoint(String name) throws SQLException {
		return new SSSavepoint(getSavepoint(), name, transactionTime);
    }
    public void rollback(Savepoint savepoint) throws SQLException {
    	if(savepoint instanceof SSSavepoint){
    		if(((SSSavepoint)savepoint).transactionTime != transactionTime){
				throw SmallSQLException.create(Language.SAVEPT_INVALID_TRANS);
    		}
    		rollback( savepoint.getSavepointId() );
    		return;
    	}
        throw SmallSQLException.create(Language.SAVEPT_INVALID_DRIVER, savepoint);
    }
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		if(savepoint instanceof SSSavepoint){
			((SSSavepoint)savepoint).transactionTime = 0;
			return;
		}
		throw SmallSQLException.create(Language.SAVEPT_INVALID_DRIVER, new Object[] { savepoint });
    }
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		return new SSStatement( this, resultSetType, resultSetConcurrency);
    }
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		return new SSPreparedStatement( this, sql);
    }
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		return new SSCallableStatement( this, sql, resultSetType, resultSetConcurrency);
    }
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        SSPreparedStatement pr = new SSPreparedStatement( this, sql);
        pr.setNeedGeneratedKeys(autoGeneratedKeys);
        return pr;
    }
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        SSPreparedStatement pr = new SSPreparedStatement( this, sql);
        pr.setNeedGeneratedKeys(columnIndexes);
        return pr;
    }
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        SSPreparedStatement pr = new SSPreparedStatement( this, sql);
        pr.setNeedGeneratedKeys(columnNames);
        return pr;
    }
	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		return null;
	}
	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return false;
	}
	@Override
	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
	}
	@Override
	public Clob createClob() throws SQLException {
		return null;
	}
	@Override
	public Blob createBlob() throws SQLException {
		return null;
	}
	@Override
	public NClob createNClob() throws SQLException {
		return null;
	}
	@Override
	public SQLXML createSQLXML() throws SQLException {
		return null;
	}
	@Override
	public boolean isValid(int timeout) throws SQLException {
		return false;
	}
	@Override
	public void setClientInfo(String name, String value)
			throws SQLClientInfoException {
	}
	@Override
	public void setClientInfo(Properties properties)
			throws SQLClientInfoException {
	}
	@Override
	public String getClientInfo(String name) throws SQLException {
		return null;
	}
	@Override
	public Properties getClientInfo() throws SQLException {
		return null;
	}
	@Override
	public Array createArrayOf(String typeName, Object[] elements)
			throws SQLException {
		return null;
	}
	@Override
	public Struct createStruct(String typeName, Object[] attributes)
			throws SQLException {
		return null;
	}
}
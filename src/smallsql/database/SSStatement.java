package smallsql.database;
import java.sql.*;
import java.util.ArrayList;
import smallsql.database.language.Language;
class SSStatement implements Statement{
    final SSConnection con;
    Command cmd;
    private boolean isClosed;
    int rsType;
    int rsConcurrency;
    private int fetchDirection;
    private int fetchSize;
    private int queryTimeout;
    private int maxRows;
    private int maxFieldSize;
    private ArrayList batches;
    private boolean needGeneratedKeys;
    private ResultSet generatedKeys;
    private int[] generatedKeyIndexes;
    private String[] generatedKeyNames;
    SSStatement(SSConnection con) throws SQLException{
        this(con, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    }
    SSStatement(SSConnection con, int rsType, int rsConcurrency) throws SQLException{
        this.con = con;
        this.rsType = rsType;
        this.rsConcurrency = rsConcurrency;
        con.testClosedConnection();
    }
    final public ResultSet executeQuery(String sql) throws SQLException{
        executeImpl(sql);
        return cmd.getQueryResult();
    }
    final public int executeUpdate(String sql) throws SQLException{
        executeImpl(sql);
        return cmd.getUpdateCount();
    }
    final public boolean execute(String sql) throws SQLException{
        executeImpl(sql);
        return cmd.getResultSet() != null;
    }
    final private void executeImpl(String sql) throws SQLException{
        checkStatement();
        generatedKeys = null;
        try{
            con.log.println(sql);
            SQLParser parser = new SQLParser();
            cmd = parser.parse(con, sql);
            if(maxRows != 0 && (cmd.getMaxRows() == -1 || cmd.getMaxRows() > maxRows))
                cmd.setMaxRows(maxRows);
            cmd.execute(con, this);
        }catch(Exception e){
            throw SmallSQLException.createFromException(e);
        }
        needGeneratedKeys = false;
        generatedKeyIndexes = null;
        generatedKeyNames = null;
    }
    final public void close(){
        con.log.println("Statement.close");
        isClosed = true;
        cmd = null;
    }
    final public int getMaxFieldSize(){
        return maxFieldSize;
    }
    final public void setMaxFieldSize(int max){
        maxFieldSize = max;
    }
    final public int getMaxRows(){
        return maxRows;
    }
    final public void setMaxRows(int max) throws SQLException{
        if(max < 0)
            throw SmallSQLException.create(Language.ROWS_WRONG_MAX, String.valueOf(max));
        maxRows = max;
    }
    final public void setEscapeProcessing(boolean enable) throws SQLException{
        checkStatement();
    }
    final public int getQueryTimeout() throws SQLException{
        checkStatement();
        return queryTimeout;
    }
    final public void setQueryTimeout(int seconds) throws SQLException{
        checkStatement();
        queryTimeout = seconds;
    }
    final public void cancel() throws SQLException{
        checkStatement();
    }
    final public SQLWarning getWarnings(){
        return null;
    }
    final public void clearWarnings(){
    }
    final public void setCursorName(String name) throws SQLException{
        throw SmallSQLException.create(Language.UNSUPPORTED_OPERATION, "setCursorName");
    }
    final public ResultSet getResultSet() throws SQLException{
        checkStatement();
        return cmd.getResultSet();
    }
    final public int getUpdateCount() throws SQLException{
        checkStatement();
        return cmd.getUpdateCount();
    }
    final public boolean getMoreResults() throws SQLException{
        checkStatement();
        return getMoreResults(CLOSE_CURRENT_RESULT);
    }
    final public void setFetchDirection(int direction) throws SQLException{
        checkStatement();
        fetchDirection = direction;
    }
    final public int getFetchDirection() throws SQLException{
        checkStatement();
        return fetchDirection;
    }
    final public void setFetchSize(int rows) throws SQLException{
        checkStatement();
        fetchSize = rows;
    }
    final public int getFetchSize() throws SQLException{
        checkStatement();
        return fetchSize;
    }
    final public int getResultSetConcurrency() throws SQLException{
        checkStatement();
        return rsConcurrency;
    }
    final public int getResultSetType() throws SQLException{
        checkStatement();
        return rsType;
    }
    final public void addBatch(String sql){
        if(batches == null)
            batches = new ArrayList();
        batches.add(sql);
    }
    public void clearBatch() throws SQLException{
        checkStatement();
        if(batches == null)
            return;
        batches.clear();
    }
    public int[] executeBatch() throws BatchUpdateException{
        if(batches == null)
            return new int[0];
        final int[] result = new int[batches.size()];
        BatchUpdateException failed = null;
        for(int i = 0; i < result.length; i++){
            try{
                result[i] = executeUpdate((String)batches.get(i));
            }catch(SQLException ex){
                result[i] = EXECUTE_FAILED;
                if(failed == null){
                    failed = new BatchUpdateException(ex.getMessage(), ex.getSQLState(), ex.getErrorCode(), result);
                    failed.initCause(ex);
                }
                failed.setNextException(ex);
            }
        }
        batches.clear();
        if(failed != null)
            throw failed;
        return result;
    }
    final public Connection getConnection(){
        return con;
    }
    final public boolean getMoreResults(int current) throws SQLException{
        switch(current){
        case CLOSE_ALL_RESULTS:
        case CLOSE_CURRENT_RESULT:
            ResultSet rs = cmd.getResultSet();
            cmd.rs = null;
            if(rs != null)
                rs.close();
            break;
        case KEEP_CURRENT_RESULT:
            break;
        default:
            throw SmallSQLException.create(Language.FLAGVALUE_INVALID, String.valueOf(current));
        }
        return cmd.getMoreResults();
    }
    final void setNeedGeneratedKeys(int autoGeneratedKeys) throws SQLException{
        switch(autoGeneratedKeys){
        case NO_GENERATED_KEYS:
            break;
        case RETURN_GENERATED_KEYS:
            needGeneratedKeys = true;
            break;
        default:
            throw SmallSQLException.create(Language.ARGUMENT_INVALID, String.valueOf(autoGeneratedKeys));
        }
    }
    final void setNeedGeneratedKeys(int[] columnIndexes) throws SQLException{
        needGeneratedKeys = columnIndexes != null;
        generatedKeyIndexes = columnIndexes;
    }
    final void setNeedGeneratedKeys(String[] columnNames) throws SQLException{
        needGeneratedKeys = columnNames != null;
        generatedKeyNames = columnNames;
    }
    final boolean needGeneratedKeys(){
        return needGeneratedKeys;
    }
    final int[] getGeneratedKeyIndexes(){
        return generatedKeyIndexes;
    }
    final String[] getGeneratedKeyNames(){
        return generatedKeyNames;
    }
    final void setGeneratedKeys(ResultSet rs){
        generatedKeys = rs;
    }
    final public ResultSet getGeneratedKeys() throws SQLException{
        if(generatedKeys == null)
            throw SmallSQLException.create(Language.GENER_KEYS_UNREQUIRED);
        return generatedKeys;
    }
    final public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException{
        setNeedGeneratedKeys(autoGeneratedKeys);
        return executeUpdate(sql);
    }
    final public int executeUpdate(String sql, int[] columnIndexes) throws SQLException{
        setNeedGeneratedKeys(columnIndexes);
        return executeUpdate(sql);
    }
    final public int executeUpdate(String sql, String[] columnNames) throws SQLException{
        setNeedGeneratedKeys(columnNames);
        return executeUpdate(sql);
    }
    final public boolean execute(String sql, int autoGeneratedKeys) throws SQLException{
        setNeedGeneratedKeys(autoGeneratedKeys);
        return execute(sql);
    }
    final public boolean execute(String sql, int[] columnIndexes) throws SQLException{
        setNeedGeneratedKeys(columnIndexes);
        return execute(sql);
    }
    final public boolean execute(String sql, String[] columnNames) throws SQLException{
        setNeedGeneratedKeys(columnNames);
        return execute(sql);
    }
    final public int getResultSetHoldability() throws SQLException{
        throw new java.lang.UnsupportedOperationException("Method getResultSetHoldability() not yet implemented.");
    }
    void checkStatement() throws SQLException{
        if(isClosed){
            throw SmallSQLException.create(Language.STMT_IS_CLOSED);
        }
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
	public boolean isClosed() throws SQLException {
		return false;
	}
	@Override
	public void setPoolable(boolean poolable) throws SQLException {
	}
	@Override
	public boolean isPoolable() throws SQLException {
		return false;
	}
}
package smallsql.database;
import java.sql.*;
import smallsql.database.language.Language;
abstract class Command {
    int type;
    String catalog;
    String name;
    SSResultSet rs;
    int updateCount = -1;
    final Expressions columnExpressions; 
    Expressions params  = new Expressions(); 
    final Logger log;
    Command(Logger log){
    	this.log = log;
		this.columnExpressions = new Expressions();
    }
	Command(Logger log, Expressions columnExpressions){
		this.log = log;
		this.columnExpressions = columnExpressions;
	}
    void addColumnExpression( Expression column ) throws SQLException{
        columnExpressions.add( column );
    }
    void addParameter( ExpressionValue param ){
        params.add( param );
    }
    void verifyParams() throws SQLException{
        for(int p=0; p<params.size(); p++){
            if(((ExpressionValue)params.get(p)).isEmpty())
            	throw SmallSQLException.create(Language.PARAM_EMPTY, new Integer(p+1));
        }
    }
    void clearParams(){
        for(int p=0; p<params.size(); p++){
            ((ExpressionValue)params.get(p)).clear();
        }
    }
	private ExpressionValue getParam(int idx) throws SQLException{
		if(idx < 1 || idx > params.size())
			throw SmallSQLException.create(Language.PARAM_IDX_OUT_RANGE, new Object[] { new Integer(idx), new Integer(params.size())});
		return ((ExpressionValue)params.get(idx-1));
	}
    void setParamValue(int idx, Object value, int dataType) throws SQLException{
		getParam(idx).set( value, dataType );
		if(log.isLogging()){
			log.println("param"+idx+'='+value+"; type="+dataType);
		}
    }
	void setParamValue(int idx, Object value, int dataType, int length) throws SQLException{
		getParam(idx).set( value, dataType, length );
		if(log.isLogging()){
			log.println("param"+idx+'='+value+"; type="+dataType+"; length="+length);
		}
	}
    final void execute(SSConnection con, SSStatement st) throws SQLException{
    	int savepoint = con.getSavepoint();
        try{
            executeImpl( con, st );
        }catch(Throwable e){
            con.rollback(savepoint);
            throw SmallSQLException.createFromException(e);
        }finally{
            if(con.getAutoCommit()) con.commit();
        }
    }
    abstract void executeImpl(SSConnection con, SSStatement st) throws Exception;
    SSResultSet getQueryResult() throws SQLException{
        if(rs == null)
        	throw SmallSQLException.create(Language.RSET_NOT_PRODUCED);
        return rs;
    }
    SSResultSet getResultSet(){
        return rs;
    }
    int getUpdateCount(){
        return updateCount;
    }
    boolean getMoreResults(){
    	rs = null;
    	updateCount = -1;
    	return false;
    }
	void setMaxRows(int max){}
    int getMaxRows(){return -1;}
}
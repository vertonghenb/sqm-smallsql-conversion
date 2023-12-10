package smallsql.database;
import java.sql.*;
import smallsql.database.language.Language;
abstract class RowSource {
	abstract boolean isScrollable();
	abstract void beforeFirst() throws Exception;
	boolean isBeforeFirst() throws SQLException{
		throw SmallSQLException.create(Language.RSET_FWDONLY);
	}
	boolean isFirst() throws SQLException{
		throw SmallSQLException.create(Language.RSET_FWDONLY);
	}
    abstract boolean first() throws Exception;
	boolean previous() throws Exception{
		throw SmallSQLException.create(Language.RSET_FWDONLY);
	}
    abstract boolean next() throws Exception;
	boolean last() throws Exception{
		throw SmallSQLException.create(Language.RSET_FWDONLY);
	}
	boolean isLast() throws Exception{
		throw SmallSQLException.create(Language.RSET_FWDONLY);
	}
	boolean isAfterLast() throws SQLException, Exception{
		throw SmallSQLException.create(Language.RSET_FWDONLY);
	}
	abstract void afterLast() throws Exception;
	boolean absolute(int row) throws Exception{
		throw SmallSQLException.create(Language.RSET_FWDONLY);
	}
	boolean relative(int rows) throws Exception{
		throw SmallSQLException.create(Language.RSET_FWDONLY);
	}
	abstract int getRow() throws Exception;
	abstract long getRowPosition();
	abstract void setRowPosition(long rowPosition) throws Exception;
	abstract void nullRow();
	abstract void noRow();
	abstract boolean rowInserted();
	abstract boolean rowDeleted();
    boolean hasAlias(){
    	return true;
    }
    void setAlias(String name) throws SQLException{
        throw SmallSQLException.create(Language.ALIAS_UNSUPPORTED);
    }
    abstract void execute() throws Exception;
    abstract boolean isExpressionsFromThisRowSource(Expressions columns);
}
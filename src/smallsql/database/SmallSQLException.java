package smallsql.database;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.MessageFormat;
import smallsql.database.language.Language;
class SmallSQLException extends SQLException {
    private static final long serialVersionUID = -1683756623665114L;
    private boolean isInit;
    private static Language language;
	private SmallSQLException(String message, String vendorCode) {
		super("[SmallSQL]" + message, vendorCode, 0);
		init();
	}
	private SmallSQLException(Throwable throwable, String message, String vendorCode) {
		super("[SmallSQL]" + message, vendorCode, 0);
		this.initCause(throwable);
		init();
	}
	private void init(){
		this.isInit = true;
		PrintWriter pw = DriverManager.getLogWriter();
		if(pw != null) this.printStackTrace(pw);	
	}
    static void setLanguage(Object localeObj) throws SQLException {
    	if (language != null && localeObj == null) return;
    	if (localeObj == null) {
    		language = Language.getDefaultLanguage(); 
    	}
    	else {
    		language = Language.getLanguage(localeObj.toString()); 
    	}
    }
	public void printStackTrace(){
		if(!isInit) return;
		super.printStackTrace();
	}
	public void printStackTrace(PrintStream ps){
		if(!isInit) return;
		super.printStackTrace(ps);
	}
	public void printStackTrace(PrintWriter pw){
		if(!isInit) return;
		super.printStackTrace(pw);
	}
    static SQLException create( String messageCode ) {
    	assert (messageCode != null): "Fill parameters";
    	String message = translateMsg(messageCode, null);
    	String sqlState = language.getSqlState(messageCode);
        return new SmallSQLException(message, sqlState);
    }
    static SQLException create( String messageCode, Object param0 ) {
    	String message = translateMsg(messageCode, new Object[] { param0 });
    	String sqlState = language.getSqlState(messageCode);
        return new SmallSQLException(message, sqlState);
    }
    static SQLException create( String messageCode, Object[] params ) {
    	String message = translateMsg(messageCode, params);
    	String sqlState = language.getSqlState(messageCode);
        return new SmallSQLException(message, sqlState);
    }
    static SQLException createFromException( Throwable e ){
        if(e instanceof SQLException) {
        	return (SQLException)e;
        }
        else {
        	String message = stripMsg(e);
        	String sqlState = language.getSqlState(Language.CUSTOM_MESSAGE);
        	return new SmallSQLException(e, message, sqlState);
        }
    }
    static SQLException createFromException( String messageCode, Object param0, 
    		Throwable e )
    {
    	String message = translateMsg(messageCode, new Object[] { param0 });
    	String sqlState = language.getSqlState(messageCode);
        return new SmallSQLException(e, message, sqlState);
    }
	static String translateMsg(String messageCode, Object[] params) {
		assert ( messageCode != null && params != null ): "Fill parameters. msgCode=" + messageCode + " params=" + params;
		String localized = language.getMessage(messageCode);		
		return MessageFormat.format(localized, params); 
	}
	private static String stripMsg(Throwable throwable) {
		String msg = throwable.getMessage();
		if(msg == null || msg.length() < 30){
			String msg2 = throwable.getClass().getName();
			msg2 = msg2.substring(msg2.lastIndexOf('.')+1);
			if(msg != null)
				msg2 = msg2 + ':' + msg;
			return msg2;
		}
		return throwable.getMessage(); 
	}
}
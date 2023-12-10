package smallsql.database;
public class CommandSet extends Command {
    int isolationLevel;
    CommandSet( Logger log, int type ){
		super(log);
        this.type = type;
    }
    void executeImpl(SSConnection con, SSStatement st) throws java.sql.SQLException {
        switch(type){
            case SQLTokenizer.LEVEL:
                con.isolationLevel = isolationLevel;
                break;
            case SQLTokenizer.USE:
            	con.setCatalog(name);
            	break;
            default:
                throw new Error();
        }
    }
}
package smallsql.database;
public class CommandCreateView extends Command{
	private Columns columns = new Columns();
	String sql;
    CommandCreateView( Logger log, String name ){
    	super(log);
        this.type = SQLTokenizer.VIEW;
        this.name = name;
    }
	void addColumn( Column column ){
		columns.add( column );
	}
    void executeImpl(SSConnection con, SSStatement st) throws Exception{
        con.getDatabase(false).createView(con, name, sql);
    }
}
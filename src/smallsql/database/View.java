package smallsql.database;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import smallsql.database.language.Language;
class View extends TableView{
	final String sql;
	final CommandSelect commandSelect;
	View(SSConnection con, String name, FileChannel raFile, long offset) throws Exception{
		super( name, new Columns() );
		StorePage storePage = new StorePage( null, -1, raFile, offset);
		StoreImpl store = StoreImpl.createStore( null, storePage, SQLTokenizer.SELECT, offset);
		sql = store.readString();
		int type;
		while((type = store.readInt()) != 0){
			int offsetInPage = store.getCurrentOffsetInPage();
			int size = store.readInt();
			switch(type){
			}
			store.setCurrentOffsetInPage(offsetInPage + size);
		}
		raFile.close();
		commandSelect = (CommandSelect)new SQLParser().parse(con, sql);
		createColumns(con);
	}
	View(Database database, SSConnection con, String name, String sql) throws Exception{
		super( name, new Columns() );
		this.sql  = sql;
		this.commandSelect = null;
		write(database, con);
	}
	View(SSConnection con, CommandSelect commandSelect) throws Exception{
		super("UNION", new Columns());
		this.sql = null;
		this.commandSelect = commandSelect;
		createColumns(con);
	}
	private void createColumns(SSConnection con) throws Exception{
		commandSelect.compile(con);
		Expressions exprs = commandSelect.columnExpressions;
		for(int c=0; c<exprs.size(); c++){
			Expression expr = exprs.get(c);
			if(expr instanceof ExpressionName){
				Column column = ((ExpressionName)expr).getColumn().copy();
				column.setName( expr.getAlias() );
				columns.add( column );
			}else{
				columns.add( new ColumnExpression(expr));
			}
		}
	}
	static void drop(Database database, String name) throws Exception{
		File file = new File( Utils.createTableViewFileName( database, name ) );
		boolean ok = file.delete();
		if(!ok) throw SmallSQLException.create(Language.VIEW_CANTDROP, name);
	}
	private void write(Database database, SSConnection con) throws Exception{
	    FileChannel raFile = createFile( con, database );
		StorePage storePage = new StorePage( null, -1, raFile, 8);
		StoreImpl store = StoreImpl.createStore( null, storePage, SQLTokenizer.CREATE, 8);
		store.writeString(sql);		
		store.writeInt( 0 ); 
		store.writeFinsh(null);
		raFile.close();
	}
	@Override
    void writeMagic(FileChannel raFile) throws Exception{
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putInt(MAGIC_VIEW);
        buffer.putInt(TABLE_VIEW_VERSION);
        buffer.position(0);
        raFile.write(buffer);
	}
}
package smallsql.database;
import java.sql.SQLException;
import java.util.ArrayList;
import smallsql.database.language.Language;
public class CommandInsert extends Command {
    boolean noColumns; 
    private CommandSelect cmdSel;
    private Table table;
    private long tableTimestamp;
    private int[] matrix;  
    CommandInsert(Logger log, String name){
        super(log);
        this.name = name;
    }
    void addColumnExpression(Expression column) throws SQLException{
        if(columnExpressions.indexOf(column) >= 0){
            throw SmallSQLException.create(Language.COL_DUPLICATE, column);
        }
        super.addColumnExpression(column);
    }
    void addValues(Expressions values){
		this.cmdSel = new CommandSelect(log, values );
    }
    void addValues( CommandSelect cmdSel ){
    	this.cmdSel = cmdSel;
    }
    private void compile(SSConnection con) throws Exception{    	
        TableView tableView = con.getDatabase(false).getTableView( con, name);
        if(!(tableView instanceof Table))
        	throw SmallSQLException.create(Language.VIEW_INSERT);
        table = (Table)tableView;
        tableTimestamp = table.getTimestamp();
		cmdSel.compile(con);
        int count = table.columns.size();
        matrix = new int[count];
        if(noColumns){
            columnExpressions.clear();
            for(int i=0; i<count; i++){
                matrix[i] = i;
            }
			if(count != cmdSel.columnExpressions.size())
					throw SmallSQLException.create(Language.COL_VAL_UNMATCH);
        }else{
            for(int i=0; i<count; i++) matrix[i] = -1;
            for(int c=0; c<columnExpressions.size(); c++){
                Expression sqlCol = columnExpressions.get(c);
                String sqlColName = sqlCol.getName();
                int idx = table.findColumnIdx( sqlColName );
                if(idx >= 0){
                    matrix[idx] = c;
                }else{
                    throw SmallSQLException.create(Language.COL_MISSING, sqlColName);
                }
            }
			if(columnExpressions.size() != cmdSel.columnExpressions.size())
					throw SmallSQLException.create(Language.COL_VAL_UNMATCH);
        }
    }
    void executeImpl(SSConnection con, SSStatement st) throws Exception {
        if(table == null || tableTimestamp != table.getTimestamp()) compile( con );
		final IndexDescriptions indexes = table.indexes;
		updateCount = 0;
		cmdSel.from.execute();
		cmdSel.beforeFirst();
        Strings keyColumnNames = null;
        ArrayList keys = null;
        boolean needGeneratedKeys = st.needGeneratedKeys();
        int generatedKeysType = 0;
        while(cmdSel.next()){
            if(needGeneratedKeys){
                keyColumnNames = new Strings();
                keys = new ArrayList();
                if(st.getGeneratedKeyNames() != null)
                    generatedKeysType = 1;
                if(st.getGeneratedKeyIndexes() != null)
                    generatedKeysType = 2;
            }
	        StoreImpl store = table.getStoreInsert( con );
	        for(int c=0; c<matrix.length; c++){
	            Column column = table.columns.get(c);
	            int idx = matrix[c];
	            Expression valueExpress;
                if(idx >= 0){
                    valueExpress = cmdSel.columnExpressions.get(idx);
                }else{
                    valueExpress = column.getDefaultValue(con);
                    if(needGeneratedKeys && generatedKeysType == 0 && valueExpress != Expression.NULL){
                        keyColumnNames.add(column.getName());
                        keys.add(valueExpress.getObject());
                    }
                }
                if(needGeneratedKeys && generatedKeysType == 1){
                    String[] keyNames = st.getGeneratedKeyNames();
                    for(int i=0; i<keyNames.length; i++){
                        if(column.getName().equalsIgnoreCase(keyNames[i])){
                            keyColumnNames.add(column.getName());
                            keys.add(valueExpress.getObject());
                            break;
                        }
                    }
                }
                if(needGeneratedKeys && generatedKeysType == 2){
                    int[] keyIndexes = st.getGeneratedKeyIndexes();
                    for(int i=0; i<keyIndexes.length; i++){
                        if(c+1 == keyIndexes[i]){
                            keyColumnNames.add(column.getName());
                            keys.add(valueExpress.getObject());
                            break;
                        }
                    }
                }
	            store.writeExpression( valueExpress, column );
				for(int i=0; i<indexes.size(); i++){
					indexes.get(i).writeExpression( c, valueExpress );
				}
	        }
	        store.writeFinsh( con );
			for(int i=0; i<indexes.size(); i++){
				indexes.get(i).writeFinish( con );
			}
	        updateCount++;
            if(needGeneratedKeys){
                Object[][] data = new Object[1][keys.size()];
                keys.toArray(data[0]);
                st.setGeneratedKeys(new SSResultSet( st, Utils.createMemoryCommandSelect( con, keyColumnNames.toArray(), data)));
            }
        }
    }
}
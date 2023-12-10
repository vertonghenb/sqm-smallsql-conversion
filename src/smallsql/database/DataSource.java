package smallsql.database;
abstract class DataSource extends RowSource{
	abstract boolean isNull( int colIdx ) throws Exception;
	abstract boolean getBoolean( int colIdx ) throws Exception;
	abstract int getInt( int colIdx ) throws Exception;
	abstract long getLong( int colIdx ) throws Exception;
	abstract float getFloat( int colIdx ) throws Exception;
	abstract double getDouble( int colIdx ) throws Exception;
	abstract long getMoney( int colIdx ) throws Exception;
	abstract MutableNumeric getNumeric( int colIdx ) throws Exception;
	abstract Object getObject( int colIdx ) throws Exception;
	abstract String getString( int colIdx ) throws Exception;
	abstract byte[] getBytes( int colIdx ) throws Exception;
	abstract int getDataType( int colIdx );
	boolean init( SSConnection con ) throws Exception{return false;}
	String getAlias(){return null;}
	abstract TableView getTableView();
	boolean isExpressionsFromThisRowSource(Expressions columns){
        for(int i=0; i<columns.size(); i++){
            ExpressionName expr = (ExpressionName)columns.get(i);
            if(this != expr.getDataSource()){
                return false;
            }
        }
        return true;
    }
}
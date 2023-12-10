package smallsql.database;
public class ExpressionName extends Expression {
    private String tableAlias;
    private DataSource fromEntry;
    private int colIdx;
    private TableView table;
    private Column column;
    ExpressionName(String name){
		super(NAME);
        setName( name );
    }
	ExpressionName(int type){
		super(type);
	}
    void setNameAfterTableAlias(String name){
        tableAlias = getName();
		setName( name );
    }
    public boolean equals(Object expr){
    	if(!super.equals(expr)) return false;
    	if(!(expr instanceof ExpressionName)) return false;
    	if( ((ExpressionName)expr).fromEntry != fromEntry) return false;
    	return true;
    }
    boolean isNull() throws Exception{
        return fromEntry.isNull(colIdx);
    }
    boolean getBoolean() throws Exception{
        return fromEntry.getBoolean(colIdx);
    }
    int getInt() throws Exception{
        return fromEntry.getInt(colIdx);
    }
    long getLong() throws Exception{
        return fromEntry.getLong(colIdx);
    }
    float getFloat() throws Exception{
        return fromEntry.getFloat(colIdx);
    }
    double getDouble() throws Exception{
        return fromEntry.getDouble(colIdx);
    }
    long getMoney() throws Exception{
        return fromEntry.getMoney(colIdx);
    }
    MutableNumeric getNumeric() throws Exception{
        return fromEntry.getNumeric(colIdx);
    }
    Object getObject() throws Exception{
        return fromEntry.getObject(colIdx);
    }
    String getString() throws Exception{
        return fromEntry.getString(colIdx);
    }
    byte[] getBytes() throws Exception{
        return fromEntry.getBytes(colIdx);
    }
    int getDataType(){
		switch(getType()){
			case NAME:
			case GROUP_BY:
				return fromEntry.getDataType(colIdx);
			case FIRST:
			case LAST:
			case MAX:
			case MIN:
			case SUM:
				return getParams()[0].getDataType();
			case COUNT:
				return SQLTokenizer.INT;
			default: throw new Error();
		}
    }
    void setFrom( DataSource fromEntry, int colIdx, TableView table ){
        this.fromEntry  = fromEntry;
        this.colIdx     = colIdx;
        this.table      = table;
        this.column		= table.columns.get(colIdx);
    }
	void setFrom( DataSource fromEntry, int colIdx, Column column ){
		this.fromEntry  = fromEntry;
		this.colIdx     = colIdx;
		this.column		= column;
	}
    DataSource getDataSource(){
        return fromEntry;
    }
    String getTableAlias(){ return tableAlias; }
	final TableView getTable(){
		return table;
	}
	final int getColumnIndex(){
		return colIdx;
	}
	final Column getColumn(){
		return column;
	}
	final public String toString(){
        if(tableAlias == null) return String.valueOf(getAlias());
        return tableAlias + "." + getAlias();
    }
	String getTableName(){
		if(table != null){
			return table.getName();
		}
		return null;
	}
	int getPrecision(){
		return column.getPrecision();
	}
	int getScale(){
		return column.getScale();
	}
	int getDisplaySize(){
		return column.getDisplaySize();
	}
	boolean isAutoIncrement(){
		return column.isAutoIncrement();
	}
	boolean isCaseSensitive(){
		return column.isCaseSensitive();
	}
	boolean isNullable(){
		return column.isNullable();
	}
	boolean isDefinitelyWritable(){
		return true;
	}
}
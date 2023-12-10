package smallsql.database;
final class Join extends RowSource{
    Expression condition; 
    private int type;
    RowSource left; 
    RowSource right;
	private boolean isAfterLast;
    private LongLongList rowPositions; 
    private int row; 
    JoinScroll scroll;
    Join( int type, RowSource left, RowSource right, Expression condition ){
        this.type = type;
        this.condition = condition;
        this.left = left;
        this.right = right;
    }
	final boolean isScrollable(){
		return false; 
	}
    void beforeFirst() throws Exception{
        scroll.beforeFirst();
		isAfterLast  = false;
		row = 0;
    }
    boolean first() throws Exception{
        beforeFirst();
        return next();
    }
    boolean next() throws Exception{
        if(isAfterLast) return false;
        row++;
        boolean result = scroll.next();
        if(!result){
            noRow();
        }
        return result;
    }
	void afterLast(){
		isAfterLast = true;
		noRow();
	}
	int getRow(){
		return row;
	}
	final long getRowPosition(){
		if(rowPositions == null) rowPositions = new LongLongList();
		rowPositions.add( left.getRowPosition(), right.getRowPosition());
		return rowPositions.size()-1;
	}
	final void setRowPosition(long rowPosition) throws Exception{
		left .setRowPosition( rowPositions.get1((int)rowPosition));
		right.setRowPosition( rowPositions.get2((int)rowPosition));
	}
	final boolean rowInserted(){
		return left.rowInserted() || right.rowInserted();
	}
	final boolean rowDeleted(){
		return left.rowDeleted() || right.rowDeleted();
	}
    void nullRow(){
    	left.nullRow();
    	right.nullRow();
    	row = 0;
    }
	void noRow(){
		isAfterLast = true;
		left.noRow();
		right.noRow();
		row = 0;
	}
    void execute() throws Exception{
    	left.execute();
    	right.execute();
        if(!createJoinScrollIndex()){
            scroll = new JoinScroll(type, left, right, condition);
        }
    }
    boolean isExpressionsFromThisRowSource(Expressions columns){
        if(left.isExpressionsFromThisRowSource(columns) || right.isExpressionsFromThisRowSource(columns)){
            return true;
        }
        if(columns.size() == 1){
            return false;
        }
        Expressions single = new Expressions();
        for(int i=0; i<columns.size(); i++){
            single.clear();
            single.add(columns.get(i));
            if(left.isExpressionsFromThisRowSource(columns) || right.isExpressionsFromThisRowSource(columns)){
                continue;
            }
            return false;
        }
        return true;
    }
    private boolean createJoinScrollIndex() throws Exception{
        if(type == CROSS_JOIN){
            return false;
        }
        if(type != INNER_JOIN){
            return false;
        }
        if(condition instanceof ExpressionArithmetic){
            ExpressionArithmetic cond = (ExpressionArithmetic)condition;
            Expressions leftEx = new Expressions();
            Expressions rightEx = new Expressions();
            int operation = createJoinScrollIndex(cond, leftEx, rightEx, 0);
            if(operation != 0){
                scroll = new JoinScrollIndex( type, left, right, leftEx, rightEx, operation);
                return true;
            }
        }
        return false;
    }
    private int createJoinScrollIndex(ExpressionArithmetic cond, Expressions leftEx, Expressions rightEx, int operation) throws Exception{
        Expression[] params = cond.getParams();
        int op = cond.getOperation();
        if(op == ExpressionArithmetic.AND){
            Expression param0 = params[0];
            Expression param1 = params[1];
            if(param0 instanceof ExpressionArithmetic && param1 instanceof ExpressionArithmetic){
                op = createJoinScrollIndex((ExpressionArithmetic)param0, leftEx, rightEx, operation);
                if(op == 0){
                    return 0;
                }
                return createJoinScrollIndex((ExpressionArithmetic)param1, leftEx, rightEx, operation);
            }
            return 0;
        }
        if(operation == 0){
            operation = op;
        }
        if(operation != op){
            return 0;
        }
        if(operation == ExpressionArithmetic.EQUALS){
            Expression param0 = params[0];
            Expression param1 = params[1];
            Expressions columns0 = Utils.getExpressionNameFromTree(param0);
            Expressions columns1 = Utils.getExpressionNameFromTree(param1);
            if(left.isExpressionsFromThisRowSource(columns0) && right.isExpressionsFromThisRowSource(columns1)){
                leftEx.add( param0 );
                rightEx.add( param1 );
            }else{
                if(left.isExpressionsFromThisRowSource(columns1) && right.isExpressionsFromThisRowSource(columns0)){
                    leftEx.add( param1 );
                    rightEx.add( param0 );
                }else{
                    return 0;
                }
            }
            return operation;
        }
        return 0;
    }
    static final int CROSS_JOIN = 1;
    static final int INNER_JOIN = 2;
    static final int LEFT_JOIN  = 3;
    static final int FULL_JOIN  = 4;
	static final int RIGHT_JOIN = 5;
}
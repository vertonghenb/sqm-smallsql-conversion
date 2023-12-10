package smallsql.database;
class JoinScrollIndex extends JoinScroll{
    private final int compare;
    Expressions leftEx;
    Expressions rightEx;
    private Index index;
    private LongTreeList rowList;
    private final LongTreeListEnum longListEnum = new LongTreeListEnum();
    JoinScrollIndex( int joinType, RowSource left, RowSource right, Expressions leftEx, Expressions rightEx, int compare)
            throws Exception{
        super( joinType, left, right, null);
        this.leftEx = leftEx;
        this.rightEx = rightEx;
        this.compare = compare;
        createIndex(rightEx);
    }
    private void createIndex(Expressions rightEx) throws Exception{
        index = new Index(false);
        right.beforeFirst();
        while(right.next()){
            index.addValues(right.getRowPosition(), rightEx);
        }
    }
    boolean next() throws Exception{
        switch(compare){
        case ExpressionArithmetic.EQUALS:
            return nextEquals();
        default:
            throw new Error("Compare operation not supported:" + compare);
        }
    }
    private boolean nextEquals() throws Exception{
        if(rowList != null){
            long rowPosition = rowList.getNext(longListEnum);
            if(rowPosition != -1){
                right.setRowPosition(rowPosition);
                return true;
            }
            rowList = null;
        }
        Object rows;
        do{
            if(!left.next()){
                return false;
            }
            rows = index.findRows(leftEx, false, null);
        }while(rows == null);
        if(rows instanceof Long){
            right.setRowPosition(((Long)rows).longValue());
        }else{
            rowList = (LongTreeList)rows;
            longListEnum.reset();
            right.setRowPosition(rowList.getNext(longListEnum));
        }
        return true;
    }
}
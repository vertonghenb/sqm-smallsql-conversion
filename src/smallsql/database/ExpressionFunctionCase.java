package smallsql.database;
final class ExpressionFunctionCase extends Expression {
	ExpressionFunctionCase() {
		super(FUNCTION);
	}
	private final Expressions cases   = new Expressions();
	private final Expressions results = new Expressions();
	private Expression elseResult = Expression.NULL;
	private int dataType = -1;
	final void addCase(Expression condition, Expression result){
		cases.add(condition);
		results.add(result);
	}
	final void setElseResult(Expression expr){
		elseResult = expr;
	}
	final void setEnd(){
		Expression[] params = new Expression[cases.size()*2 + (elseResult!=null ? 1 : 0)];
		int i=0;
		for(int p=0; p<cases.size(); p++){
			params[i++] = cases  .get( p );
			params[i++] = results.get( p );
		}
		if(i<params.length)
			params[i] = elseResult;
		super.setParams(params);
	}
	final void setParams( Expression[] params ){
		super.setParams(params);
		int i = 0;
		for(int p=0; p<cases.size(); p++){
			cases  .set( p, params[i++]);
			results.set( p, params[i++]);
		}
		if(i<params.length)
			elseResult = params[i];
	}
    void setParamAt( Expression param, int idx){
    	super.setParamAt( param, idx );
    	int p = idx / 2;
    	if(p>=cases.size()){
    		elseResult = param;
    		return;
    	}
    	if(idx % 2 > 0){    		
    		results.set( p, param );
    	}else{
    		cases.set( p, param );
    	}
    }
	final int getFunction() {
		return SQLTokenizer.CASE;
	}
	final boolean isNull() throws Exception {
		return getResult().isNull();
	}
	final boolean getBoolean() throws Exception {
		return getResult().getBoolean();
	}
	final int getInt() throws Exception {
		return getResult().getInt();
	}
	final long getLong() throws Exception {
		return getResult().getLong();
	}
	final float getFloat() throws Exception {
		return getResult().getFloat();
	}
	final double getDouble() throws Exception {
		return getResult().getDouble();
	}
	final long getMoney() throws Exception {
		return getResult().getMoney();
	}
	final MutableNumeric getNumeric() throws Exception {
		return getResult().getNumeric();
	}
	final Object getObject() throws Exception {
		return getResult().getObject();
	}
	final String getString() throws Exception {
		return getResult().getString();
	}
	final byte[] getBytes() throws Exception{
		return getResult().getBytes();
	}
	final int getDataType() {
		if(dataType < 0){
			dataType = elseResult.getDataType();
			for(int i=0; i<results.size(); i++){
				dataType = ExpressionArithmetic.getDataType(dataType, results.get(i).getDataType());
			}
		}
		return dataType;
	}
	final int getPrecision(){
		int precision = 0;
		for(int i=results.size()-1; i>=0; i--){
			precision = Math.max(precision, results.get(i).getPrecision());
		}
		return precision;
	}
	final int getScale(){
		int precision = 0;
		for(int i=results.size()-1; i>=0; i--){
			precision = Math.max(precision, results.get(i).getScale());
		}
		return precision;
	}
	final private Expression getResult() throws Exception{
		for(int i=0; i<cases.size(); i++){
			if(cases.get(i).getBoolean()) return results.get(i);
		}
		return elseResult;
	}
}
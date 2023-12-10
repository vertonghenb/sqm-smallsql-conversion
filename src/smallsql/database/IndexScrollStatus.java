package smallsql.database;
class IndexScrollStatus {
	private final IndexNode rootPage;
	private final Expressions expressions; 
	private final java.util.Stack nodeStack = new java.util.Stack(); 
	private LongTreeList longList;
	private LongTreeListEnum longListEnum = new LongTreeListEnum();
	IndexScrollStatus(IndexNode rootPage, Expressions expressions){	
		this.rootPage	= rootPage;
		this.expressions= expressions;
		reset();
	}
	final void reset(){
		nodeStack.clear();
		boolean asc = (expressions.get(0).getAlias() != SQLTokenizer.DESC_STR);
		nodeStack.push( new IndexNodeScrollStatus(rootPage, asc, true, 0) );
	}
	final long getRowOffset( boolean scroll){
		if(longList != null){
			long rowOffset = scroll ? 
								longList.getNext(longListEnum) : 
								longList.getPrevious(longListEnum);
			if(rowOffset < 0){
				longList = null;
			}else{
				return rowOffset;
			}
		}
		while(true){
			IndexNodeScrollStatus status = (IndexNodeScrollStatus)nodeStack.peek();
			int level = status.level;
			if(!status.asc ^ scroll){
				int idx = ++status.idx;
				if(idx == -1){
					if(status.nodeValue != null){
						if(status.nodeValue instanceof IndexNode){
							level++;
							nodeStack.push(
								new IndexNodeScrollStatus( 	(IndexNode)status.nodeValue, 
														(expressions.get(level).getAlias() != SQLTokenizer.DESC_STR), 
														scroll, level));
							continue;
						}else
							return getReturnValue(status.nodeValue);
					}
					idx = ++status.idx;
				}
				if(idx >= status.nodes.length){
					if(nodeStack.size() > 1){
						nodeStack.pop();
						continue;
					}else{
                        status.idx = status.nodes.length; 
						return -1;
					}
				}
				IndexNode node = status.nodes[idx];
				nodeStack.push( new IndexNodeScrollStatus(node, status.asc, scroll, level) );
			}else{
				int idx = --status.idx;
				if(idx == -1){
					if(status.nodeValue != null){
						if(status.nodeValue instanceof IndexNode){
							level++;
							nodeStack.push(
								new IndexNodeScrollStatus( 	(IndexNode)status.nodeValue, 
														(expressions.get(level).getAlias() != SQLTokenizer.DESC_STR), 
														scroll, level));
							continue;
						}else
							return getReturnValue(status.nodeValue);
					}
				}
				if(idx < 0){
					if(nodeStack.size() > 1){
						nodeStack.pop();
						continue;
					}else{
						return -1;
					}
				}
				IndexNode node = status.nodes[idx];
				nodeStack.push( new IndexNodeScrollStatus(node, status.asc, scroll, level) );
			}
		}
	}
	final void afterLast(){
		longList = null;
		nodeStack.setSize(1);
		((IndexNodeScrollStatus)nodeStack.peek()).afterLast();
	}
	private final long getReturnValue( Object value){
		if(rootPage.getUnique()){
			return ((Long)value).longValue();
		}else{
			longList = (LongTreeList)value;
			longListEnum.reset();
			return longList.getNext(longListEnum); 
		}
	}
}
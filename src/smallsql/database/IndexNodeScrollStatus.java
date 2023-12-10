package smallsql.database;
final class IndexNodeScrollStatus {
	final boolean asc;
	final IndexNode[] nodes;
	int idx;
	final Object nodeValue;
	final int level;
	IndexNodeScrollStatus(IndexNode node, boolean asc, boolean scroll, int level){
		this.nodes = node.getChildNodes();
		nodeValue = node.getValue();
		this.asc = asc;
		this.idx = (asc ^ scroll) ? nodes.length : -2;
		this.level = level;
	}
	void afterLast(){
		idx = (asc) ? nodes.length : -2;			
	}
}
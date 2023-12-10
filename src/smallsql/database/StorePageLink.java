package smallsql.database;
class StorePageLink {
	long filePos;
	TableStorePage page;
	StoreImpl getStore(Table table, SSConnection con, int lock) throws Exception{
		TableStorePage page = this.page;
		if(page == null)
			return table.getStore( con, filePos, lock );
		while(page.nextLock != null) page = page.nextLock;
		return table.getStore( page, lock);
	}
}
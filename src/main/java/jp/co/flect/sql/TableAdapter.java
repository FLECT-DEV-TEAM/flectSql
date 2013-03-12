package jp.co.flect.sql;

import java.util.EventListener;

public class TableAdapter implements TableListener {
	
	public void beforeInsert(TableEvent e) {}
	public void afterInsert(TableEvent e) {}
	
	public void beforeUpdate(TableEvent e) {}
	public void afterUpdate(TableEvent e) {}
	
	public void beforeDelete(TableEvent e) {}
	public void afterDelete(TableEvent e) {}
	
}

package jp.co.flect.sql;

import java.util.EventListener;

public interface TableListener extends EventListener {
	
	public void beforeInsert(TableEvent e);
	public void afterInsert(TableEvent e);
	
	public void beforeUpdate(TableEvent e);
	public void afterUpdate(TableEvent e);
	
	public void beforeDelete(TableEvent e);
	public void afterDelete(TableEvent e);
	
}

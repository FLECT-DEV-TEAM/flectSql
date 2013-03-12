package jp.co.flect.sql;

import java.util.EventObject;

public class TableEvent extends EventObject {
	
	public static final int BEFORE_INSERT = 1;
	public static final int AFTER_INSERT  = 2;
	public static final int BEFORE_UPDATE = 11;
	public static final int AFTER_UPDATE  = 12;
	public static final int BEFORE_DELETE = 21;
	public static final int AFTER_DELETE  = 22;
	
	public TableEvent(Table table) {
		super(table);
	}
	
	public Table getTable() { return (Table)getSource();}
}

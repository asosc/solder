package org.solder.core;

import java.util.Objects;

import com.ee.session.db.Event.EventDefinition;
import com.ee.session.db.Event.Priority;

public enum SEvent implements EventDefinition  {
	
	
	
	SolderMain_Init(201,Priority.MEDIUM),DbUpdateFail(202,Priority.MED_HIGH),
	DbDeleteFail(203,Priority.MED_HIGH);
	
	//More to come as we 
	
	
	int id,priority;

	SEvent(int id,int priority) {
		this.id=id;
		this.priority = priority;
		
	}
	
	SEvent(int id,Priority ep) {
		Objects.requireNonNull(ep);
		this.id=id;
		this.priority=ep.priority();
	}
	
	public String getName() {
		return name();
	}
	
	public int id() {
		return id;
	}
	
	
	public int getPriority() {
		return priority;
	}

}
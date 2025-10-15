package org.solder.core;

import java.util.Objects;

import com.ee.session.db.Audit.ActionDefinition;
import com.ee.session.db.Audit.AuditLogLocation;

public enum SAudit  implements ActionDefinition  {
	SVault_Create(201),SVault_Update(202),SVault_Delete(203);
	
	
	int id;
	AuditLogLocation location;
	SAudit(int id) {
		this.id=id;
		this.location=AuditLogLocation.FILEDB;
	}
	
	SAudit(int id,AuditLogLocation location) {
		Objects.requireNonNull(location);
		this.id=id;
		this.location=location;
	}
	
	public String getName() {
		return name();
	}
	
	public int id() {
		return id;
	}
	
	
	public AuditLogLocation getLocation() {
		return location;
	}

}

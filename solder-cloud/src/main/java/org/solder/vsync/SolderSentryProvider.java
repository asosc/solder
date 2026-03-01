package org.solder.vsync;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import com.ee.session.db.Audit.AuditLevel;
import com.ee.session.db.RolePriv;
import com.ee.session.db.RolePriv.PrivilegeDefinition;
import com.ee.session.db.RolePriv.RoleDefinition;
import com.ee.session.db.SentryProvider;
import com.lnk.lucene.RunOnce;

public class SolderSentryProvider extends SentryProvider {
	
	//Only 2 system roles.. All other has to be done by the application.
	//Solder will use ROLE_ID 100-150
		public static final int ROLEID_SOLDER_READONLY = 100;
		public static final int ROLEID_SOLDER_COMMITER = 101;
		public static final int ROLEID_SOLDER_ADMIN = 102;


		// catch all ops ..
		public static final String SOLDEROP_SOLDER_ADMIN = "sadmin";
		public static final String SOLDEROP_READ = "sread";
		public static final String SOLDEROP_WRITE = "swrite";
		

		public static final String SENTRY_SOLDER = "solder_sentry";

		// Only 2 roles. everything should be done by apps.
		static AtomicBoolean s_fInit = new AtomicBoolean(false);

		static void init() throws IOException {
			RunOnce.ensure(s_fInit, () -> {
				// First create statc roles.
				RolePriv.registerNamedOps(SOLDEROP_SOLDER_ADMIN, SOLDEROP_READ,SOLDEROP_WRITE);
				new SolderSentryProvider();
			});
		}

		static Set<RoleDefinition> defineSystemRoles() {
			return Set.of(
					new RoleDefinition(ROLEID_SOLDER_ADMIN, "solder_admin", SENTRY_SOLDER, null, Set.of(SOLDEROP_SOLDER_ADMIN,SOLDEROP_READ,SOLDEROP_WRITE),
							AuditLevel.MEDIUM, AuditLevel.HIGH),
					new RoleDefinition(ROLEID_SOLDER_READONLY, "solder_read", SENTRY_SOLDER, null,
							Set.of(SOLDEROP_READ), AuditLevel.MEDIUM, AuditLevel.HIGH),
					new RoleDefinition(ROLEID_SOLDER_COMMITER, "solder_commit", SENTRY_SOLDER, null,
							Set.of(SOLDEROP_WRITE,SOLDEROP_READ), AuditLevel.MEDIUM, AuditLevel.HIGH));
		}

		
		SolderSentryProvider() {
				super(SENTRY_SOLDER, defineSystemRoles(),new HashSet<PrivilegeDefinition>());
			
		}

}

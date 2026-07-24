-- Auto Generated SQL Tables --- 

CREATE TABLE registry (
	tenant_id int NOT NULL,
	rkey nvarchar(255) NOT NULL,
	value_split1 nvarchar(255) NULL,
	value_split2 nvarchar(255) NULL,
	value_split3 nvarchar(255) NULL,
	create_date datetime NOT NULL,
	last_update datetime NOT NULL,
	CONSTRAINT pk_registry PRIMARY KEY (tenant_id,rkey) ON EeFileGroup
) ON EeFileGroup;

CREATE TABLE elock (
	name nvarchar(255) NOT NULL,
	pid nvarchar(255) NOT NULL,
	locked tinyint NOT NULL,
	nonce bigint NULL,
	lock_app nvarchar(255) NULL,
	timeout bigint NOT NULL,
	lock_date datetime NULL,
	touch_date datetime NULL,
	create_date datetime NOT NULL,
	last_update datetime NOT NULL,
	CONSTRAINT pk_elock PRIMARY KEY (name) ON EeFileGroup
) ON EeFileGroup;


CREATE SEQUENCE tenant_seq START WITH 1 INCREMENT BY 1 NO CYCLE NO CACHE;

CREATE TABLE tenant (
	id int NOT NULL,
	name nvarchar(255) NOT NULL,
	dname nvarchar(255) NULL,
	root_domain_id int NOT NULL,
	tgroup nvarchar(255) NULL,
	create_date datetime NOT NULL,
	last_update datetime NOT NULL,
	info_split1 nvarchar(255) NULL,
	info_split2 nvarchar(255) NULL,
	info_split3 nvarchar(255) NULL,
	CONSTRAINT pk_tenant PRIMARY KEY (id) ON EeFileGroup,
	CONSTRAINT uk1_tenant UNIQUE(name) ON EeFileGroup,
	CONSTRAINT uk2_tenant UNIQUE(root_domain_id) ON EeFileGroup
) ON EeFileGroup;

CREATE TABLE role (
	user_id int NOT NULL,
	role_id int NOT NULL,
	scope int NOT NULL,
	domain_ids nvarchar(255) NULL,
	create_date datetime NOT NULL,
	last_update datetime NOT NULL,
	CONSTRAINT pk_role PRIMARY KEY (user_id,role_id) ON EeFileGroup
) ON EeFileGroup;


CREATE SEQUENCE service_acct_seq START WITH 1 INCREMENT BY 1 NO CYCLE NO CACHE;

CREATE TABLE service_acct (
	id int NOT NULL,
	name nvarchar(255) NOT NULL,
	prov nvarchar(255) NOT NULL,
	create_date datetime NOT NULL,
	last_update datetime NOT NULL,
	info_split1 nvarchar(255) NULL,
	info_split2 nvarchar(255) NULL,
	info_split3 nvarchar(255) NULL,
	info_split4 nvarchar(255) NULL,
	info_split5 nvarchar(255) NULL,
	info_split6 nvarchar(255) NULL,
	CONSTRAINT pk_service_ac PRIMARY KEY (id) ON EeFileGroup,
	CONSTRAINT uk_service_ac UNIQUE(name) ON EeFileGroup
) ON EeFileGroup;


CREATE SEQUENCE domain_seq START WITH 1 INCREMENT BY 1 NO CYCLE NO CACHE;

CREATE TABLE domain (
	id int NOT NULL,
	tenant_id int NOT NULL,
	name nvarchar(255) NOT NULL,
	dname nvarchar(255) NULL,
	parent_id int NOT NULL,
	create_date datetime NOT NULL,
	last_update datetime NOT NULL,
	CONSTRAINT pk_domain PRIMARY KEY (id) ON EeFileGroup,
	CONSTRAINT uk_domain UNIQUE(tenant_id,name) ON EeFileGroup
) ON EeFileGroup;


CREATE SEQUENCE srepo_seq START WITH 1 INCREMENT BY 1 NO CYCLE NO CACHE;

CREATE TABLE srepo (
	sid int NOT NULL,
	id nvarchar(255) NOT NULL,
	tschema nvarchar(128) NOT NULL,
	tenant_id int NOT NULL,
	ao_id int NOT NULL,
	tag nvarchar(255) NULL,
	deleted tinyint NOT NULL,
	commit_dir nvarchar(255) NULL,
	ext_keep nvarchar(255) NOT NULL,
	commit_id int NOT NULL,
	commit_date datetime NOT NULL,
	change_date datetime NOT NULL,
	create_date datetime NOT NULL,
	last_update datetime NOT NULL,
	CONSTRAINT pk_srepo PRIMARY KEY (id) ON EeFileGroup,
	CONSTRAINT uk1_srepo UNIQUE(tenant_id,tschema,ao_id) ON EeFileGroup,
	CONSTRAINT uk2_srepo UNIQUE(sid) ON EeFileGroup
) ON EeFileGroup;

CREATE TABLE privilege (
	user_id int NOT NULL,
	priv_id int NOT NULL,
	priv_obj_id int NOT NULL,
	create_date datetime NOT NULL,
	CONSTRAINT pk_privilege PRIMARY KEY (user_id,priv_id,priv_obj_id) ON EeFileGroup
) ON EeFileGroup;


CREATE SEQUENCE user_seq START WITH 1 INCREMENT BY 1 NO CYCLE NO CACHE;

CREATE TABLE users (
	id int NOT NULL,
	tenant_id int NOT NULL,
	name nvarchar(255) NOT NULL,
	domain_id int NOT NULL,
	full_name nvarchar(255) NULL,
	email nvarchar(255) NOT NULL,
	ext_id nvarchar(255) NULL,
	create_date datetime NOT NULL,
	last_update datetime NOT NULL,
	info_split1 nvarchar(255) NULL,
	info_split2 nvarchar(255) NULL,
	info_split3 nvarchar(255) NULL,
	CONSTRAINT pk_users PRIMARY KEY (id) ON EeFileGroup,
	CONSTRAINT uk1_users UNIQUE(tenant_id,name) ON EeFileGroup,
	CONSTRAINT uk2_users UNIQUE(tenant_id,email) ON EeFileGroup
) ON EeFileGroup;

CREATE INDEX i1_users  ON users(domain_id);


CREATE SEQUENCE tsnap_seq START WITH 1 INCREMENT BY 1 NO CYCLE NO CACHE;

CREATE TABLE tsnap (
	id int NOT NULL,
	name nvarchar(64) NOT NULL,
	subset nvarchar(255) NOT NULL,
	criteria nvarchar(255) NOT NULL,
	next_criteria nvarchar(255) NOT NULL,
	create_date datetime NOT NULL,
	props_split1 nvarchar(255) NULL,
	props_split2 nvarchar(255) NULL,
	props_split3 nvarchar(255) NULL,
	n_import int NOT NULL,
	n_ignored int NOT NULL,
	n_total int NOT NULL,
	CONSTRAINT pk_tsnap PRIMARY KEY (id) ON EeFileGroup
) ON EeFileGroup;

CREATE INDEX i1_tsnap  ON tsnap(name,subset,create_date);


CREATE SEQUENCE scommit_seq START WITH 1 INCREMENT BY 1 NO CYCLE NO CACHE;

CREATE TABLE scommit (
	id int NOT NULL,
	repo_id nvarchar(48) NOT NULL,
	chash nvarchar(128) NOT NULL,
	prev_id int NOT NULL,
	prev_chash nvarchar(128) NOT NULL,
	tenant_id int NOT NULL,
	blob_fsid bigint NOT NULL,
	create_date datetime NOT NULL,
	info_split1 nvarchar(255) NULL,
	info_split2 nvarchar(255) NULL,
	info_split3 nvarchar(255) NULL,
	CONSTRAINT pk_scommit PRIMARY KEY (id) ON EeFileGroup,
	CONSTRAINT uk_scommit UNIQUE(repo_id,prev_id,chash) ON EeFileGroup
) ON EeFileGroup;


CREATE SEQUENCE container_seq START WITH 1 INCREMENT BY 1 NO CYCLE NO CACHE;

CREATE TABLE container (
	id int NOT NULL,
	name nvarchar(255) NOT NULL,
	cgroup nvarchar(255) NOT NULL,
	prov nvarchar(255) NOT NULL,
	service_acct_id int NOT NULL,
	path nvarchar(255) NOT NULL,
	cflags int NOT NULL,
	create_date datetime NOT NULL,
	last_update datetime NOT NULL,
	info_split1 nvarchar(255) NULL,
	info_split2 nvarchar(255) NULL,
	info_split3 nvarchar(255) NULL,
	CONSTRAINT pk_container PRIMARY KEY (id) ON EeFileGroup,
	CONSTRAINT uk_container UNIQUE(name) ON EeFileGroup
) ON EeFileGroup;

CREATE INDEX i1_container  ON container(cgroup);


CREATE SEQUENCE machine_seq START WITH 1 INCREMENT BY 1 NO CYCLE NO CACHE;

CREATE TABLE machine (
	id int NOT NULL,
	name nvarchar(255) NOT NULL,
	mgroup nvarchar(255) NULL,
	create_date datetime NOT NULL,
	last_update datetime NOT NULL,
	info_split1 nvarchar(255) NULL,
	info_split2 nvarchar(255) NULL,
	info_split3 nvarchar(255) NULL,
	CONSTRAINT pk_machine PRIMARY KEY (id) ON EeFileGroup,
	CONSTRAINT uk_machine UNIQUE(name) ON EeFileGroup
) ON EeFileGroup;

CREATE TABLE enckeys (
	id int NOT NULL,
	id_enc int NOT NULL,
	kgroup nvarchar(255) NOT NULL,
	ekey_split1 nvarchar(255) NULL,
	ekey_split2 nvarchar(255) NULL,
	ekey_split3 nvarchar(255) NULL,
	ekey_split4 nvarchar(255) NULL,
	ekey_split5 nvarchar(255) NULL,
	ekey_split6 nvarchar(255) NULL,
	create_date datetime NOT NULL,
	CONSTRAINT pk_enckeys PRIMARY KEY (id) ON EeFileGroup
) ON EeFileGroup;

CREATE INDEX i1_enckeys  ON enckeys(kgroup);


CREATE SEQUENCE tgroup_seq START WITH 1 INCREMENT BY 1 NO CYCLE NO CACHE;

CREATE TABLE tgroup (
	id int NOT NULL,
	name nvarchar(255) NOT NULL,
	create_date datetime NOT NULL,
	CONSTRAINT pk_tgroup PRIMARY KEY (id) ON EeFileGroup,
	CONSTRAINT uk_tgroup UNIQUE(name) ON EeFileGroup
) ON EeFileGroup;

CREATE TABLE login (
	user_id int NOT NULL,
	login_type nvarchar(255) NOT NULL,
	tenant_id int NOT NULL,
	secret_split1 nvarchar(255) NULL,
	secret_split2 nvarchar(255) NULL,
	secret_split3 nvarchar(255) NULL,
	secret_split4 nvarchar(255) NULL,
	secret_split5 nvarchar(255) NULL,
	secret_split6 nvarchar(255) NULL,
	secret_split7 nvarchar(255) NULL,
	secret_split8 nvarchar(255) NULL,
	create_date datetime NOT NULL,
	last_update datetime NOT NULL,
	CONSTRAINT pk_login PRIMARY KEY (user_id,login_type) ON EeFileGroup
) ON EeFileGroup;


CREATE SEQUENCE auth_seq START WITH 1 INCREMENT BY 1 NO CYCLE NO CACHE;

CREATE TABLE auth (
	id int NOT NULL,
	type nvarchar(255) NOT NULL,
	user_id int NOT NULL,
	tenant_id int NOT NULL,
	client_id int NOT NULL,
	decision int NOT NULL,
	token nvarchar(255) NULL,
	user_login nvarchar(255) NULL,
	info_split1 nvarchar(255) NULL,
	info_split2 nvarchar(255) NULL,
	info_split3 nvarchar(255) NULL,
	info_split4 nvarchar(255) NULL,
	create_date datetime NOT NULL,
	CONSTRAINT pk_auth PRIMARY KEY (id) ON EeFileGroup
) ON EeFileGroup;

CREATE INDEX i1_auth  ON auth(tenant_id,create_date,decision);

CREATE TABLE ualias (
	tenant_id int NOT NULL,
	alias nvarchar(255) NOT NULL,
	id int NOT NULL,
	domain_id int NOT NULL,
	type nvarchar(255) NOT NULL,
	create_date datetime NOT NULL,
	last_update datetime NOT NULL,
	CONSTRAINT pk_ualias PRIMARY KEY (tenant_id,alias) ON EeFileGroup
) ON EeFileGroup;

CREATE INDEX i1_ualias  ON ualias(id);

CREATE TABLE eprocess (
	id nvarchar(255) NOT NULL,
	name nvarchar(255) NOT NULL,
	version nvarchar(255) NOT NULL,
	pid nvarchar(255) NOT NULL,
	create_date datetime NOT NULL,
	info_split1 nvarchar(255) NULL,
	info_split2 nvarchar(255) NULL,
	info_split3 nvarchar(255) NULL,
	info_split4 nvarchar(255) NULL,
	CONSTRAINT pk_eprocess PRIMARY KEY (id) ON EeFileGroup
) ON EeFileGroup;

CREATE INDEX i1_eprocess  ON eprocess(create_date,name);

CREATE TABLE audit (
	id bigint NOT NULL,
	action_id int NOT NULL,
	level int NOT NULL,
	create_date datetime NOT NULL,
	user_id int NOT NULL,
	domain_id int NOT NULL,
	tenant_id int NOT NULL,
	user_name nvarchar(255) NULL,
	ao_id bigint NOT NULL,
	ao_domain_id int NOT NULL,
	ao_tenant_id int NOT NULL,
	sess_id nvarchar(255) NOT NULL,
	client_id int NOT NULL,
	trans_id bigint NOT NULL,
	auth_id int NULL,
	auth_type nvarchar(255) NULL,
	auth_token nvarchar(255) NULL,
	pid nvarchar(255) NOT NULL,
	local_ip nvarchar(255) NULL,
	remote_ip nvarchar(255) NULL,
	info_split1 nvarchar(255) NULL,
	info_split2 nvarchar(255) NULL,
	info_split3 nvarchar(255) NULL,
	info_split4 nvarchar(255) NULL
) ON EeFileGroup;

CREATE INDEX i1_audit  ON audit(id);

CREATE INDEX i2_audit  ON audit(tenant_id,create_date);


CREATE SEQUENCE eapikey_seq START WITH 1 INCREMENT BY 1 NO CYCLE NO CACHE;

CREATE TABLE eapikey (
	id int NOT NULL,
	user_id int NOT NULL,
	tenant_id int NOT NULL,
	key_id nvarchar(255) NOT NULL,
	key_name nvarchar(255) NOT NULL,
	create_date datetime NOT NULL,
	expiry_date datetime NULL,
	secret_split1 nvarchar(255) NULL,
	secret_split2 nvarchar(255) NULL,
	secret_split3 nvarchar(255) NULL,
	secret_split4 nvarchar(255) NULL,
	last_update datetime NOT NULL,
	CONSTRAINT pk_eapikey PRIMARY KEY (id) ON EeFileGroup,
	CONSTRAINT uk1_eapikey UNIQUE(key_id) ON EeFileGroup,
	CONSTRAINT uk2_eapikey UNIQUE(user_id,key_name) ON EeFileGroup
) ON EeFileGroup;


CREATE SEQUENCE blob_fs_seq START WITH 1 INCREMENT BY 1 NO CYCLE NO CACHE;

CREATE TABLE blob_fs (
	id bigint NOT NULL,
	name nvarchar(255) NOT NULL,
	c_id int NOT NULL,
	vloc nvarchar(255) NOT NULL,
	owner_app nvarchar(255) NOT NULL,
	owner_ref nvarchar(255) NOT NULL,
	tenant_id int NOT NULL,
	tgroup int NOT NULL,
	user_id int NOT NULL,
	ext_id int NOT NULL,
	create_date datetime NOT NULL,
	last_update datetime NOT NULL,
	info_split1 nvarchar(255) NULL,
	info_split2 nvarchar(255) NULL,
	info_split3 nvarchar(255) NULL,
	info_split4 nvarchar(255) NULL,
	info_split5 nvarchar(255) NULL,
	info_split6 nvarchar(255) NULL,
	CONSTRAINT pk_blob_fs PRIMARY KEY (id) ON EeFileGroup,
	CONSTRAINT uk_blob_fs UNIQUE(name) ON EeFileGroup
) ON EeFileGroup;

CREATE INDEX i1_blob_fs  ON blob_fs(owner_app,owner_ref);

CREATE INDEX i2_blob_fs  ON blob_fs(c_id,create_date);

CREATE TABLE blob (
	blob_id nvarchar(255) NOT NULL,
	fs_id bigint NOT NULL,
	c_id int NOT NULL,
	vloc nvarchar(255) NOT NULL,
	owner_app nvarchar(255) NOT NULL,
	owner_ref nvarchar(255) NOT NULL,
	tenant_id int NOT NULL,
	tgroup int NOT NULL,
	user_id int NOT NULL,
	ext_id int NOT NULL,
	create_date datetime NOT NULL,
	last_update datetime NOT NULL,
	info_split1 nvarchar(255) NULL,
	info_split2 nvarchar(255) NULL,
	info_split3 nvarchar(255) NULL,
	info_split4 nvarchar(255) NULL,
	info_split5 nvarchar(255) NULL,
	info_split6 nvarchar(255) NULL,
	CONSTRAINT pk_blob PRIMARY KEY (blob_id) ON EeFileGroup
) ON EeFileGroup;

CREATE INDEX i1_blob  ON blob(fs_id);

CREATE INDEX i2_blob  ON blob(c_id,create_date);

CREATE INDEX i3_blob  ON blob(tgroup,owner_app,create_date);

CREATE TABLE cg_reg (
	name nvarchar(255) NOT NULL,
	cgroup nvarchar(255) NULL,
	create_date datetime NOT NULL,
	last_update datetime NOT NULL,
	CONSTRAINT pk_cg_reg PRIMARY KEY (name) ON EeFileGroup
) ON EeFileGroup;

CREATE TABLE session (
	id nvarchar(255) NOT NULL,
	user_id int NOT NULL,
	tenant_id int NOT NULL,
	sess_key nvarchar(255) NOT NULL,
	nonce bigint NOT NULL,
	auth_type nvarchar(255) NOT NULL,
	auth_id int NOT NULL,
	auth_token nvarchar(255) NULL,
	auth_client_id int NULL,
	state int NOT NULL,
	record_level int NOT NULL,
	view_level int NOT NULL,
	create_date datetime NOT NULL,
	touch_date datetime NOT NULL,
	info_split1 nvarchar(255) NULL,
	info_split2 nvarchar(255) NULL,
	info_split3 nvarchar(255) NULL,
	info_split4 nvarchar(255) NULL,
	last_update datetime NOT NULL,
	CONSTRAINT pk_session PRIMARY KEY (id) ON EeFileGroup
) ON EeFileGroup;

CREATE INDEX i1_session  ON session(tenant_id,create_date);


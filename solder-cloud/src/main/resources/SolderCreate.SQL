CREATE TABLE srepo (
	id nvarchar(48) NOT NULL,
	tschema nvarchar(16) NOT NULL,
	tenant_id int NOT NULL,
	ao_id int NOT NULL,
	commit_dir nvarchar(255) NULL,
	ext_keep nvarchar(255) NOT NULL,
	commit_id int NOT NULL,
	commit_date datetime NOT NULL,
	change_date datetime NOT NULL,
	create_date datetime NOT NULL,
	last_update datetime NOT NULL,
	CONSTRAINT pk_srepo PRIMARY KEY (id) ON EeFileGroup,
	CONSTRAINT uk_srepo UNIQUE(tschema,tenant_id,ao_id) ON EeFileGroup
) ON EeFileGroup;

CREATE SEQUENCE scommit_seq START WITH 1 INCREMENT BY 1 NO CYCLE NO CACHE;

CREATE TABLE scommit (
	id int NOT NULL,
	repo_id nvarchar(48) NOT NULL,
	chash nvarchar(128) NOT NULL,
	tenant_id int NOT NULL,
	create_date datetime NOT NULL,
	info_split1 nvarchar(255) NULL,
	info_split2 nvarchar(255) NULL,
	info_split3 nvarchar(255) NULL,
	CONSTRAINT pk_scommit PRIMARY KEY (id) ON EeFileGroup,
	CONSTRAINT uk_scommit UNIQUE(repo_id,chash) ON EeFileGroup
) ON EeFileGroup;
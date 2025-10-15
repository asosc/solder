CREATE TABLE svault (
	id nvarchar(48) NOT NULL,
	tschema nvarchar(16) NOT NULL,
	tenant_id int NOT NULL,
	ao_id int NOT NULL,
	rep_info_split1 nvarchar(255) NULL,
	rep_info_split2 nvarchar(255) NULL,
	rep_info_split3 nvarchar(255) NULL,
	rep_date datetime NOT NULL,
	change_date datetime NOT NULL,
	create_date datetime NOT NULL,
	last_update datetime NOT NULL,
	CONSTRAINT pk_svault PRIMARY KEY (id) ON EeFileGroup,
	CONSTRAINT uk_svault UNIQUE(tschema,tenant_id,ao_id) ON EeFileGroup
) ON EeFileGroup;

-- public.ducklake_column definition

-- Drop table

-- DROP TABLE public.ducklake_column;

CREATE TABLE public.ducklake_column (
	column_id int8 NULL,
	begin_snapshot int8 NULL,
	end_snapshot int8 NULL,
	table_id int8 NULL,
	column_order int8 NULL,
	column_name varchar NULL,
	column_type varchar NULL,
	initial_default varchar NULL,
	default_value varchar NULL,
	nulls_allowed bool NULL,
	parent_column int8 NULL,
	default_value_type varchar DEFAULT 'literal'::character varying NULL,
	default_value_dialect varchar NULL
);


-- public.ducklake_column_mapping definition

-- Drop table

-- DROP TABLE public.ducklake_column_mapping;

CREATE TABLE public.ducklake_column_mapping (
	mapping_id int8 NULL,
	table_id int8 NULL,
	"type" varchar NULL
);


-- public.ducklake_column_tag definition

-- Drop table

-- DROP TABLE public.ducklake_column_tag;

CREATE TABLE public.ducklake_column_tag (
	table_id int8 NULL,
	column_id int8 NULL,
	begin_snapshot int8 NULL,
	end_snapshot int8 NULL,
	"key" varchar NULL,
	value varchar NULL
);


-- public.ducklake_data_file definition

-- Drop table

-- DROP TABLE public.ducklake_data_file;

CREATE TABLE public.ducklake_data_file (
	data_file_id int8 NOT NULL,
	table_id int8 NULL,
	begin_snapshot int8 NULL,
	end_snapshot int8 NULL,
	file_order int8 NULL,
	"path" varchar NULL,
	path_is_relative bool NULL,
	file_format varchar NULL,
	record_count int8 NULL,
	file_size_bytes int8 NULL,
	footer_size int8 NULL,
	row_id_start int8 NULL,
	partition_id int8 NULL,
	encryption_key varchar NULL,
	mapping_id int8 NULL,
	partial_max int8 NULL,
	CONSTRAINT ducklake_data_file_pkey PRIMARY KEY (data_file_id)
);
CREATE INDEX ducklake_data_file_compaction_idx ON public.ducklake_data_file USING btree (table_id, end_snapshot, file_size_bytes) WHERE (end_snapshot IS NULL);
CREATE INDEX ducklake_data_file_compaction_order_idx ON public.ducklake_data_file USING btree (table_id, end_snapshot, file_size_bytes, begin_snapshot, row_id_start, data_file_id) WHERE (end_snapshot IS NULL);
CREATE INDEX ducklake_data_file_snapshot_read_idx ON public.ducklake_data_file USING btree (table_id, begin_snapshot, end_snapshot);


-- public.ducklake_delete_file definition

-- Drop table

-- DROP TABLE public.ducklake_delete_file;

CREATE TABLE public.ducklake_delete_file (
	delete_file_id int8 NOT NULL,
	table_id int8 NULL,
	begin_snapshot int8 NULL,
	end_snapshot int8 NULL,
	data_file_id int8 NULL,
	"path" varchar NULL,
	path_is_relative bool NULL,
	format varchar NULL,
	delete_count int8 NULL,
	file_size_bytes int8 NULL,
	footer_size int8 NULL,
	encryption_key varchar NULL,
	partial_max int8 NULL,
	CONSTRAINT ducklake_delete_file_pkey PRIMARY KEY (delete_file_id)
);
CREATE INDEX ducklake_delete_file_snapshot_read_idx ON public.ducklake_delete_file USING btree (table_id, begin_snapshot, end_snapshot);
CREATE INDEX ducklake_delete_file_table_idx ON public.ducklake_delete_file USING btree (table_id, end_snapshot) WHERE (end_snapshot IS NULL);


-- public.ducklake_file_column_stats definition

-- Drop table

-- DROP TABLE public.ducklake_file_column_stats;

CREATE TABLE public.ducklake_file_column_stats (
	data_file_id int8 NULL,
	table_id int8 NULL,
	column_id int8 NULL,
	column_size_bytes int8 NULL,
	value_count int8 NULL,
	null_count int8 NULL,
	min_value varchar NULL,
	max_value varchar NULL,
	contains_nan bool NULL,
	extra_stats varchar NULL
);
CREATE INDEX ducklake_file_column_stats_file_idx ON public.ducklake_file_column_stats USING btree (data_file_id);


-- public.ducklake_file_partition_value definition

-- Drop table

-- DROP TABLE public.ducklake_file_partition_value;

CREATE TABLE public.ducklake_file_partition_value (
	data_file_id int8 NULL,
	table_id int8 NULL,
	partition_key_index int8 NULL,
	partition_value varchar NULL
);
CREATE INDEX ducklake_file_partition_value_file_idx ON public.ducklake_file_partition_value USING btree (data_file_id);
CREATE INDEX ducklake_file_partition_value_table_idx ON public.ducklake_file_partition_value USING btree (table_id);
CREATE INDEX ducklake_file_partition_value_cover_idx ON public.ducklake_file_partition_value USING btree (data_file_id, partition_key_index, partition_value);


-- public.ducklake_file_variant_stats definition

-- Drop table

-- DROP TABLE public.ducklake_file_variant_stats;

CREATE TABLE public.ducklake_file_variant_stats (
	data_file_id int8 NULL,
	table_id int8 NULL,
	column_id int8 NULL,
	variant_path varchar NULL,
	shredded_type varchar NULL,
	column_size_bytes int8 NULL,
	value_count int8 NULL,
	null_count int8 NULL,
	min_value varchar NULL,
	max_value varchar NULL,
	contains_nan bool NULL,
	extra_stats varchar NULL
);


-- public.ducklake_files_scheduled_for_deletion definition

-- Drop table

-- DROP TABLE public.ducklake_files_scheduled_for_deletion;

CREATE TABLE public.ducklake_files_scheduled_for_deletion (
	data_file_id int8 NULL,
	"path" varchar NULL,
	path_is_relative bool NULL,
	schedule_start timestamptz NULL
);


-- public.ducklake_inlined_data_11_32 definition

-- Drop table

-- DROP TABLE public.ducklake_inlined_data_11_32;

CREATE TABLE public.ducklake_inlined_data_11_32 (
	row_id int8 NULL,
	begin_snapshot int8 NULL,
	end_snapshot int8 NULL,
	"type" bytea NULL,
	x int8 NULL,
	y int8 NULL,
	pointer_target_fixed bool NULL,
	viewport_height int8 NULL,
	viewport_width int8 NULL,
	current_url bytea NULL,
	session_id bytea NULL,
	scale_factor int8 NULL,
	"timestamp" bytea NULL,
	team_id int8 NULL,
	distinct_id bytea NULL,
	_inserted_at varchar NULL
);


-- public.ducklake_inlined_data_13_30 definition

-- Drop table

-- DROP TABLE public.ducklake_inlined_data_13_30;

CREATE TABLE public.ducklake_inlined_data_13_30 (
	row_id int8 NULL,
	begin_snapshot int8 NULL,
	end_snapshot int8 NULL,
	person_id bytea NULL,
	team_id int8 NULL,
	distinct_id bytea NULL,
	"version" int8 NULL,
	is_deleted int8 NULL,
	_inserted_at varchar NULL
);


-- public.ducklake_inlined_data_15_29 definition

-- Drop table

-- DROP TABLE public.ducklake_inlined_data_15_29;

CREATE TABLE public.ducklake_inlined_data_15_29 (
	row_id int8 NULL,
	begin_snapshot int8 NULL,
	end_snapshot int8 NULL,
	destination_id bytea NULL,
	instance_id bytea NULL,
	last_snapshot_id int8 NULL,
	last_replicated_at varchar NULL,
	rows_replicated int8 NULL,
	last_error bytea NULL,
	last_error_at varchar NULL,
	updated_at varchar NULL
);


-- public.ducklake_inlined_data_5_31 definition

-- Drop table

-- DROP TABLE public.ducklake_inlined_data_5_31;

CREATE TABLE public.ducklake_inlined_data_5_31 (
	row_id int8 NULL,
	begin_snapshot int8 NULL,
	end_snapshot int8 NULL,
	"uuid" bytea NULL,
	"event" bytea NULL,
	properties bytea NULL,
	"timestamp" bytea NULL,
	team_id int8 NULL,
	project_id int8 NULL,
	distinct_id bytea NULL,
	elements_chain bytea NULL,
	created_at bytea NULL,
	captured_at bytea NULL,
	person_id bytea NULL,
	person_properties bytea NULL,
	person_created_at bytea NULL,
	person_mode bytea NULL,
	_inserted_at varchar NULL,
	group0_properties bytea NULL,
	group1_properties bytea NULL,
	group2_properties bytea NULL,
	group3_properties bytea NULL,
	group4_properties bytea NULL,
	group0_created_at bytea NULL,
	group1_created_at bytea NULL,
	group2_created_at bytea NULL,
	group3_created_at bytea NULL,
	group4_created_at bytea NULL,
	historical_migration bool NULL
);


-- public.ducklake_inlined_data_tables definition

-- Drop table

-- DROP TABLE public.ducklake_inlined_data_tables;

CREATE TABLE public.ducklake_inlined_data_tables (
	table_id int8 NULL,
	table_name varchar NULL,
	schema_version int8 NULL
);


-- public.ducklake_macro definition

-- Drop table

-- DROP TABLE public.ducklake_macro;

CREATE TABLE public.ducklake_macro (
	schema_id int8 NULL,
	macro_id int8 NULL,
	macro_name varchar NULL,
	begin_snapshot int8 NULL,
	end_snapshot int8 NULL
);


-- public.ducklake_macro_impl definition

-- Drop table

-- DROP TABLE public.ducklake_macro_impl;

CREATE TABLE public.ducklake_macro_impl (
	macro_id int8 NULL,
	impl_id int8 NULL,
	dialect varchar NULL,
	"sql" varchar NULL,
	"type" varchar NULL
);


-- public.ducklake_macro_parameters definition

-- Drop table

-- DROP TABLE public.ducklake_macro_parameters;

CREATE TABLE public.ducklake_macro_parameters (
	macro_id int8 NULL,
	impl_id int8 NULL,
	column_id int8 NULL,
	parameter_name varchar NULL,
	parameter_type varchar NULL,
	default_value varchar NULL,
	default_value_type varchar NULL
);


-- public.ducklake_metadata definition

-- Drop table

-- DROP TABLE public.ducklake_metadata;

CREATE TABLE public.ducklake_metadata (
	"key" varchar NOT NULL,
	value varchar NOT NULL,
	"scope" varchar NULL,
	scope_id int8 NULL
);


-- public.ducklake_name_mapping definition

-- Drop table

-- DROP TABLE public.ducklake_name_mapping;

CREATE TABLE public.ducklake_name_mapping (
	mapping_id int8 NULL,
	column_id int8 NULL,
	source_name varchar NULL,
	target_field_id int8 NULL,
	parent_column int8 NULL,
	is_partition bool NULL
);


-- public.ducklake_partition_column definition

-- Drop table

-- DROP TABLE public.ducklake_partition_column;

CREATE TABLE public.ducklake_partition_column (
	partition_id int8 NULL,
	table_id int8 NULL,
	partition_key_index int8 NULL,
	column_id int8 NULL,
	"transform" varchar NULL
);


-- public.ducklake_partition_info definition

-- Drop table

-- DROP TABLE public.ducklake_partition_info;

CREATE TABLE public.ducklake_partition_info (
	partition_id int8 NULL,
	table_id int8 NULL,
	begin_snapshot int8 NULL,
	end_snapshot int8 NULL
);


-- public.ducklake_schema definition

-- Drop table

-- DROP TABLE public.ducklake_schema;

CREATE TABLE public.ducklake_schema (
	schema_id int8 NOT NULL,
	schema_uuid uuid NULL,
	begin_snapshot int8 NULL,
	end_snapshot int8 NULL,
	schema_name varchar NULL,
	"path" varchar NULL,
	path_is_relative bool NULL,
	CONSTRAINT ducklake_schema_pkey PRIMARY KEY (schema_id)
);


-- public.ducklake_schema_versions definition

-- Drop table

-- DROP TABLE public.ducklake_schema_versions;

CREATE TABLE public.ducklake_schema_versions (
	begin_snapshot int8 NULL,
	schema_version int8 NULL,
	table_id int8 NULL
);


-- public.ducklake_snapshot definition

-- Drop table

-- DROP TABLE public.ducklake_snapshot;

CREATE TABLE public.ducklake_snapshot (
	snapshot_id int8 NOT NULL,
	snapshot_time timestamptz NULL,
	schema_version int8 NULL,
	next_catalog_id int8 NULL,
	next_file_id int8 NULL,
	CONSTRAINT ducklake_snapshot_pkey PRIMARY KEY (snapshot_id)
);


-- public.ducklake_snapshot_changes definition

-- Drop table

-- DROP TABLE public.ducklake_snapshot_changes;

CREATE TABLE public.ducklake_snapshot_changes (
	snapshot_id int8 NOT NULL,
	changes_made varchar NULL,
	author varchar NULL,
	commit_message varchar NULL,
	commit_extra_info varchar NULL,
	CONSTRAINT ducklake_snapshot_changes_pkey PRIMARY KEY (snapshot_id)
);


-- public.ducklake_sort_expression definition

-- Drop table

-- DROP TABLE public.ducklake_sort_expression;

CREATE TABLE public.ducklake_sort_expression (
	sort_id int8 NULL,
	table_id int8 NULL,
	sort_key_index int8 NULL,
	"expression" varchar NULL,
	dialect varchar NULL,
	sort_direction varchar NULL,
	null_order varchar NULL
);


-- public.ducklake_sort_info definition

-- Drop table

-- DROP TABLE public.ducklake_sort_info;

CREATE TABLE public.ducklake_sort_info (
	sort_id int8 NULL,
	table_id int8 NULL,
	begin_snapshot int8 NULL,
	end_snapshot int8 NULL
);


-- public.ducklake_table definition

-- Drop table

-- DROP TABLE public.ducklake_table;

CREATE TABLE public.ducklake_table (
	table_id int8 NULL,
	table_uuid uuid NULL,
	begin_snapshot int8 NULL,
	end_snapshot int8 NULL,
	schema_id int8 NULL,
	table_name varchar NULL,
	"path" varchar NULL,
	path_is_relative bool NULL
);


-- public.ducklake_table_column_stats definition

-- Drop table

-- DROP TABLE public.ducklake_table_column_stats;

CREATE TABLE public.ducklake_table_column_stats (
	table_id int8 NULL,
	column_id int8 NULL,
	contains_null bool NULL,
	contains_nan bool NULL,
	min_value varchar NULL,
	max_value varchar NULL,
	extra_stats varchar NULL
);


-- public.ducklake_table_stats definition

-- Drop table

-- DROP TABLE public.ducklake_table_stats;

CREATE TABLE public.ducklake_table_stats (
	table_id int8 NULL,
	record_count int8 NULL,
	next_row_id int8 NULL,
	file_size_bytes int8 NULL
);


-- public.ducklake_tag definition

-- Drop table

-- DROP TABLE public.ducklake_tag;

CREATE TABLE public.ducklake_tag (
	object_id int8 NULL,
	begin_snapshot int8 NULL,
	end_snapshot int8 NULL,
	"key" varchar NULL,
	value varchar NULL
);


-- public.ducklake_view definition

-- Drop table

-- DROP TABLE public.ducklake_view;

CREATE TABLE public.ducklake_view (
	view_id int8 NULL,
	view_uuid uuid NULL,
	begin_snapshot int8 NULL,
	end_snapshot int8 NULL,
	schema_id int8 NULL,
	view_name varchar NULL,
	dialect varchar NULL,
	"sql" varchar NULL,
	column_aliases varchar NULL
);
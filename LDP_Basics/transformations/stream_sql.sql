CREATE OR REFRESH STREAMING TABLE sql_stream_table
AS 
SELECT * FROM STREAM azuredatabricks_catalog.ldp_basics.source_a;


CREATE OR REFRESH STREAMING TABLE sql_stream_depends
AS 
SELECT * FROM STREAM(sql_stream_table);

CREATE OR REFRESH MATERIALIZED VIEW sql_mat_view
AS 
SELECT * FROM sql_stream_table;
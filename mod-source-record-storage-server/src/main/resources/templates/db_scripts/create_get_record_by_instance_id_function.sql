-- Custom script to create a function to get the MARC record by instanceId. Changes in this file will not result in an update of the function.
-- To change the function, update this script and copy it to the appropriate scripts.snippet field of the schema.json

CREATE OR REPLACE FUNCTION get_record_by_instance_id(instanceId uuid)
RETURNS jsonb AS $rcrd$
DECLARE
	rcrd jsonb;
BEGIN
WITH rslt AS (SELECT *
		FROM
		(SELECT *
			FROM
			(SELECT *
			  FROM records_view
			  WHERE records_view.jsonb ->> 'recordType' = 'MARC')  AS mrc
			CROSS JOIN LATERAL json_array_elements(jsonb -> 'parsedRecord' -> 'content' -> 'fields') fields(field)
		    WHERE field ->> '999' IS NOT null) AS tmp
		CROSS JOIN LATERAL json_array_elements(field -> '999' -> 'subfields') ids(identifier)
		WHERE (identifier ->> 'i')::uuid = instanceId)

SELECT jsonb into rcrd
	FROM rslt
	WHERE (jsonb ->> 'generation')::int = (SELECT MAX((jsonb ->> 'generation')::int) FROM rslt);
RETURN rcrd;
END;
$rcrd$ LANGUAGE plpgsql;
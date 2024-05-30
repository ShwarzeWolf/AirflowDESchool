CREATE TABLE biogrid_interactors(
    "interactor_a" INT NOT NULL,
    "interactors_b" VARCHAR(64) ARRAY NOT NULL,
    CONSTRAINT pk_biogrid_interactors PRIMARY KEY ("interactor_a")
);


CREATE OR REPLACE FUNCTION get_biogrid_interactors()
RETURNS VOID
LANGUAGE plpgsql
AS $BODY$
BEGIN

    TRUNCATE TABLE biogrid_interactors;

    INSERT INTO biogrid_interactors
    SELECT
        bd."biogrid_id_interactor_a" as interactor_a,
        ARRAY_AGG(bd."biogrid_id_interactor_b") as interactors_b
    FROM biogrid_data bd
    GROUP BY bd."biogrid_id_interactor_a";

END;
$BODY$;

SELECT get_biogrid_interactors();
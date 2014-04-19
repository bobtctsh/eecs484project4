-- Test selection queries

CREATE INDEX DA (ikey);
SELECT * FROM DA, DB WHERE DA.ikey = DB.ikey; -- use INL
SELECT * FROM DA, DB WHERE DA.serial = DB.dkey; -- use INL
SELECT DA.serial, DA.filler, DB.serial, DB.filler FROM DA, DB WHERE DA.dkey = DB.dkey; 
SELECT * FROM DA, DB WHERE DA.filler = DB.filler; 
SELECT DA.ikey, DB.serial FROM DA, DB WHERE DA.ikey < DB.serial; -- use SNL

DROP INDEX DA (ikey);
SELECT * FROM DA, DB WHERE DA.ikey = DB.ikey; -- use SMJ
SELECT * FROM DA, DB WHERE DA.serial = DB.dkey; -- use SMJ
SELECT DA.serial, DA.filler, DB.serial, DB.filler FROM DA, DB WHERE DA.dkey = DB.dkey; 
SELECT * FROM DA, DB WHERE DA.filler = DB.filler; 

SELECT DA.ikey, DB.serial FROM DA, DB WHERE DA.ikey < DB.filler; -- use SNL


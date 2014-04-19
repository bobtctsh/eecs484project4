--
-- test Insert with and without indices
--

INSERT INTO DA (serial, ikey, filler, dkey) VALUES (101, 11618, '00000 string record', 1.1);
INSERT INTO DA (serial, ikey, filler, dkey) VALUES (102, 11618, '00000 string record',  2.2);

SELECT * FROM DA;

--
-- test Insert with indices
--
CREATE INDEX DB (ikey);
CREATE INDEX DB (ikey);
CREATE INDEX DB (serial);
CREATE INDEX DB (filler);
CREATE INDEX DB (dkey);

INSERT INTO DB (serial, ikey, filler, dkey) VALUES (101, 11618, '00000 string record',  0.0);
INSERT INTO DB (serial, ikey, filler, dkey) VALUES (102, 11618, '00000 string record',  2.2);


SELECT * FROM DB WHERE DB.serial <10;  -- use index select
SELECT DB.serial,DB.ikey,DB.dkey FROM DB WHERE DB.ikey >= 10000;
SELECT DB.serial,DB.ikey,DB.dkey FROM DB WHERE DB.dkey = 0.7;
SELECT * FROM DB WHERE DB.filler = '00000 string record';

DROP INDEX DB (ikey);
DROP INDEX DB (ikey);
DROP INDEX DB (serial);
DROP INDEX DB (filler);
DROP INDEX DB (dkey);

SELECT * FROM DB WHERE DB.serial < 10; 
SELECT DB.serial,DB.ikey,DB.dkey FROM DB WHERE DB.ikey >= 10000;
SELECT DB.serial,DB.ikey,DB.dkey FROM DB WHERE DB.dkey = 0.7;
SELECT * FROM DB WHERE DB.filler = '00000 string record';

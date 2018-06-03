BEGIN

UPDATE Accounts SET val = val + :delta WHERE id = :aid;

SELECT val FROM Accounts WHERE id = :aid;

UPDATE Tellers SET val = val + :delta WHERE id = :tid;

UPDATE Branches SET val = val + :delta WHERE id = :bid;

INSERT INTO History (id, tid, bid, aid, delta) VALUES (:id, :tid, :bid, :aid, :delta);

COMMIT


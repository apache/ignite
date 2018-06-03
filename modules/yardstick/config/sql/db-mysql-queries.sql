BEGIN

UPDATE Accounts SET val = val + :delta WHERE id = :aid;

SELECT val FROM Accounts WHERE id = :aid;

UPDATE Tellers SET val = val + :delta WHERE id = :tid;

UPDATE Branches SET val = val + :delta WHERE id = :bid;

INSERT INTO History (tid, bid, aid, delta) VALUES (:tid, :bid, :aid, :delta);

COMMIT


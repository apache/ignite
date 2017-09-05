UPDATE "Accounts".Accounts SET val = val + :delta WHERE _key = :aid;

SELECT val FROM "Accounts".Accounts WHERE _key = :aid;

UPDATE "Tellers".Tellers SET val = val + :delta WHERE _key = :tid;

UPDATE "Branches".Branches SET val = val + :delta WHERE _key = :bid;

INSERT INTO "History".History (_key, tid, bid, aid, delta) VALUES (?, ?, ?, ? ,?);

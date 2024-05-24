DROP TABLE IF EXISTS stuart_user, stuart_acl;

CREATE TABLE stuart_user(
	id varchar(32) NOT NULL,
	username varchar(255) NOT NULL,
	password varchar(255) NOT NULL,
	PRIMARY KEY (id),
	UNIQUE (username)
);

COMMENT ON COLUMN stuart_user.id is 'id';
COMMENT ON COLUMN stuart_user.username is 'username';
COMMENT ON COLUMN stuart_user.password is 'password';

CREATE TABLE stuart_acl (
  id varchar(32) NOT NULL,
  seq bigint NOT NULL,
  target varchar(255) NOT NULL,
  type integer NOT NULL,
  topic varchar(255) NOT NULL,
  authority varchar(20) NOT NULL,
  PRIMARY KEY (id),
  UNIQUE (target, topic)
);

COMMENT ON COLUMN stuart_acl.id is 'id';
COMMENT ON COLUMN stuart_acl.id is 'sequence';
COMMENT ON COLUMN stuart_acl.target is 'filter target: username, ipaddr, clientid, $all';
COMMENT ON COLUMN stuart_acl.type is 'filter target type: username(1), ipaddr(2), clientid(3), all(4)';
COMMENT ON COLUMN stuart_acl.topic is 'filter topic';
COMMENT ON COLUMN stuart_acl.authority is 'authority: sub|deny, pub|deny, subpub|deny, sub|allow, pub|allow, subpub|allow';

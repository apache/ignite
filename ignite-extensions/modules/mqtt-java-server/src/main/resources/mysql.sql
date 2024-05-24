DROP TABLE IF EXISTS `stuart_user`, `stuart_acl`;

CREATE TABLE `stuart_user` (
  `id` varchar(32) NOT NULL COMMENT 'id',
  `username` varchar(255) NOT NULL COMMENT 'username',
  `password` varchar(255) NOT NULL COMMENT 'password',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_stuart_user` (`username`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `stuart_acl` (
  `id` varchar(32) NOT NULL COMMENT 'id',
  `seq` bigint NOT NULL COMMENT 'sequence',
  `target` varchar(255) NOT NULL COMMENT 'filter target: username, ipaddr, clientid, $all',
  `type` int(1) NOT NULL COMMENT 'filter target type: username(1), ipaddr(2), clientid(3), all(4)',
  `topic` varchar(255) NOT NULL COMMENT 'filter topic',
  `authority` varchar(20) NOT NULL COMMENT 'authority: sub|deny, pub|deny, subpub|deny, sub|allow, pub|allow, subpub|allow',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_stuart_acl` (`target`, `topic`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

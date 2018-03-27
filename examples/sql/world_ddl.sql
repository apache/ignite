DROP TABLE IF EXISTS Country;

CREATE TABLE Country (
  Code CHAR(3) PRIMARY KEY,
  Name CHAR(52),
  Continent CHAR(50),
  Region CHAR(26),
  SurfaceArea DECIMAL(10,2),
  IndepYear SMALLINT(6),
  Population INT(11),
  LifeExpectancy DECIMAL(3,1),
  GNP DECIMAL(10,2),
  GNPOld DECIMAL(10,2),
  LocalName CHAR(45),
  GovernmentForm CHAR(45),
  HeadOfState CHAR(60),
  Capital INT(11),
  Code2 CHAR(2)
) WITH "template=partitioned, backups=1, CACHE_NAME=Country, VALUE_TYPE=demo.model.Country";

DROP TABLE IF EXISTS City;

CREATE TABLE City (
  ID INT(11),
  Name CHAR(35),
  CountryCode CHAR(3),
  District CHAR(20),
  Population INT(11),
  PRIMARY KEY (ID, CountryCode)
) WITH "template=partitioned, backups=1, affinityKey=CountryCode, CACHE_NAME=City, KEY_TYPE=demo.model.CityKey, VALUE_TYPE=demo.model.City";

CREATE INDEX idx_country_code ON city (CountryCode);

DROP TABLE IF EXISTS CountryLanguage;

CREATE TABLE CountryLanguage (
  CountryCode CHAR(3),
  Language CHAR(30),
  IsOfficial CHAR(2),
  Percentage DECIMAL(4,1),
  PRIMARY KEY (CountryCode, Language)
) WITH "template=partitioned, backups=1, affinityKey=CountryCode, CACHE_NAME=CountryLng, KEY_TYPE=demo.model.CountryLngKey, VALUE_TYPE=demo.model.CountryLng";

CREATE INDEX idx_lang_country_code ON CountryLanguage (CountryCode);
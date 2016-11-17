SELECT Person.firstName  FROM "query".Person, "orgCache".Organization WHERE Person.orgId = Organization.id AND lower(Organization.name) = lower('Organization 55')
SELECT Organization.name  FROM "orgCache".Organization WHERE lower(Organization.name) LIKE lower('%55%')
SELECT Person.firstName  FROM "query".Person, "orgCache".Organization WHERE Person.orgId = Organization.id AND lower(Organization.name) = lower('Organization 55') #distributedJoins

using System;
using Apache.Ignite.Core;
using Apache.Ignite.Core.Cache;
using Apache.Ignite.Core.Cache.Affinity;
using Apache.Ignite.Core.Cache.Configuration;

namespace dotnet_helloworld
{
    // tag::affinityCollocation[]
    class Person
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public int CityId { get; set; }
        public string CompanyId { get; set; }
    }

    class PersonKey
    {
        public int Id { get; set; }

        [AffinityKeyMapped] public string CompanyId { get; set; }
    }

    class Company
    {
        public string Name { get; set; }
    }

    class AffinityCollocation
    {
        public static void Example()
        {
            var personCfg = new CacheConfiguration
            {
                Name = "persons",
                Backups = 1,
                CacheMode = CacheMode.Partitioned
            };

            var companyCfg = new CacheConfiguration
            {
                Name = "companies",
                Backups = 1,
                CacheMode = CacheMode.Partitioned
            };

            using (var ignite = Ignition.Start())
            {
                var personCache = ignite.GetOrCreateCache<PersonKey, Person>(personCfg);
                var companyCache = ignite.GetOrCreateCache<string, Company>(companyCfg);

                var person = new Person {Name = "Vasya"};

                var company = new Company {Name = "Company1"};

                personCache.Put(new PersonKey {Id = 1, CompanyId = "company1_key"}, person);
                companyCache.Put("company1_key", company);
            }
        }
    }
    // end::affinityCollocation[]

    static class CacheKeyConfigurationExamples
    {
        public static void ConfigureAffinityKeyWithCacheKeyConfiguration() {
            // tag::config-with-key-configuration[]
            var personCfg = new CacheConfiguration("persons")
            {
                KeyConfiguration = new[]
                {
                    new CacheKeyConfiguration
                    {
                        TypeName = nameof(Person),
                        AffinityKeyFieldName = nameof(Person.CompanyId)
                    } 
                }
            };

            var companyCfg = new CacheConfiguration("companies");

            IIgnite ignite = Ignition.Start();

            ICache<PersonKey, Person> personCache = ignite.GetOrCreateCache<PersonKey, Person>(personCfg);
            ICache<string, Company> companyCache = ignite.GetOrCreateCache<string, Company>(companyCfg);

            var companyId = "company_1";
            Company c1 = new Company {Name = "My company"};
            Person p1 = new Person {Id = 1, Name = "John", CompanyId = companyId};

            // Both the p1 and c1 objects will be cached on the same node
            personCache.Put(new PersonKey {Id = 1, CompanyId = companyId}, p1);
            companyCache.Put(companyId, c1);

            // Get the person object
            p1 = personCache.Get(new PersonKey {Id = 1, CompanyId = companyId});
            // end::config-with-key-configuration[]
        }

        public static void AffinityKeyClass()
        {
            // tag::affinity-key-class[]
            var personCfg = new CacheConfiguration("persons");
            var companyCfg = new CacheConfiguration("companies");

            IIgnite ignite = Ignition.Start();

            ICache<AffinityKey, Person> personCache = ignite.GetOrCreateCache<AffinityKey, Person>(personCfg);
            ICache<string, Company> companyCache = ignite.GetOrCreateCache<string, Company>(companyCfg);

            var companyId = "company_1";
            Company c1 = new Company {Name = "My company"};
            Person p1 = new Person {Id = 1, Name = "John", CompanyId = companyId};

            // Both the p1 and c1 objects will be cached on the same node
            personCache.Put(new AffinityKey(1, companyId), p1);
            companyCache.Put(companyId, c1);

            // Get the person object
            p1 = personCache.Get(new AffinityKey(1, companyId));
            // end::affinity-key-class[]
        }
    }
}
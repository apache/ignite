using Apache.Ignite.Core;
using Apache.Ignite.Core.Cache.Configuration;

namespace dotnet_helloworld
{
    //todo discuss about "Indexing Nested Objects"
    public class DefiningIndexes
    {
        class Person
        {
            // tag::idxAnnotationCfg[]
            // Indexed field. Will be visible to the SQL engine.
            [QuerySqlField(IsIndexed = true)] public long Id;

            //Queryable field. Will be visible to the SQL engine
            [QuerySqlField] public string Name;

            //Will NOT be visible to the SQL engine.
            public int Age;
            /**
              * Indexed field sorted in descending order.
              * Will be visible to the SQL engine.
            */
            [QuerySqlField(IsIndexed = true, IsDescending = true)]
            public float Salary;
            // end::idxAnnotationCfg[]
        }
        
        //todo indexing nested objects - will be deprecated, discuss with Artem

        public static void RegisteringIndexedTypes()
        {
            // tag::registeringIndexedTypes[]
            //looks like it's unsupported in dotnet
            // end::registeringIndexedTypes[]
        }

        public class GroupIndexes
        {
            //todo functional is limited comparing to Java client, discuss
            // tag::groupIdx[]
            class Person
            {
                [QuerySqlField(IndexGroups = new[] {"age_salary_idx"})]
                public int Age;

                [QuerySqlField(IsIndexed = true, IndexGroups = new[] {"age_salary_idx"})]
                public double Salary;
            }
            // end::groupIdx[]
        }

        public static void QueryEntityDemo()
        {
            // tag::queryEntity[]
            var cacheCfg = new CacheConfiguration
            {
                Name = "myCache",
                QueryEntities = new[]
                {
                    new QueryEntity
                    {
                        KeyType = typeof(long),
                        KeyFieldName = "id",
                        ValueType = typeof(dotnet_helloworld.Person),
                        Fields = new[]
                        {
                            new QueryField
                            {
                                Name = "id",
                                FieldType = typeof(long)
                            },   
                            new QueryField
                            {
                                Name = "name",
                                FieldType = typeof(string)
                            },   
                            new QueryField
                            {
                                Name = "salary",
                                FieldType = typeof(long)
                            },   
                        },
                        Indexes = new[]
                        {
                            new QueryIndex("name"),
                            new QueryIndex(false, QueryIndexType.Sorted, new[] {"id", "salary"})
                        }
                    }
                }
            };
            Ignition.Start(new IgniteConfiguration
            {
                CacheConfiguration = new[] {cacheCfg}
            });
            // end::queryEntity[]
        }
    }
}

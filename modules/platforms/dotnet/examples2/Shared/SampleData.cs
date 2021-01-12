using System.Collections.Generic;
using IgniteExamples.Shared.Models;

namespace IgniteExamples.Shared
{
    public static class SampleData
    {
        public static IEnumerable<Employee> GetEmployees()
        {
            // TODO: Use faker library?
            yield return new Employee(
                "James Wilson",
                12500,
                new Address("1096 Eddy Street, San Francisco, CA", 94109),
                new[] {"Human Resources", "Customer Service"},
                1);
        }
    }
}

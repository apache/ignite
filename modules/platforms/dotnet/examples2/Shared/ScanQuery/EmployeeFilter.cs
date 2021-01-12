using Apache.Ignite.Core.Cache;
using IgniteExamples.Shared.Models;

namespace IgniteExamples.Shared.ScanQuery
{
    public class EmployeeFilter : ICacheEntryFilter<int, Employee>
    {
        public string EmployeeName { get; set; }

        public bool Invoke(ICacheEntry<int, Employee> entry)
        {
            return EmployeeName == null || entry.Value.Name.Contains(EmployeeName);
        }
    }
}

using Apache.Ignite.Core;

namespace dotnet_helloworld
{
    public class UnderstandingSchemas
    {
        public static void MultipleSchemas()
        {
            // tag::schemas[]
            var cfg = new IgniteConfiguration
            {
                SqlSchemas = new[]
                {
                    "MY_SCHEMA",
                    "MY_SECOND_SCHEMA"
                }
            };
            // end::schemas[]
        }
    }
}
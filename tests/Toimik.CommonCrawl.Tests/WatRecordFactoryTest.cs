namespace Toimik.CommonCrawl.Tests
{
    using System;
    using Toimik.WarcProtocol;
    using Xunit;

    public class WatRecordFactoryTest
    {
        [Theory]
        [InlineData(ContinuationRecord.TypeName, false)]
        [InlineData(ConversionRecord.TypeName, false)]
        [InlineData(RequestRecord.TypeName, false)]
        [InlineData(ResourceRecord.TypeName, false)]
        [InlineData(ResponseRecord.TypeName, false)]
        [InlineData(RevisitRecord.TypeName, false)]
        [InlineData(WarcinfoRecord.TypeName, false)]
        [InlineData(MetadataRecord.TypeName, true)]
        public void CreateRecord(string recordType, bool isTransformed)
        {
            var factory = new WatRecordFactory(hostname: "www.example.com");

            var record = factory.CreateRecord(
                version: "1.0",
                recordType,
                new Uri($"urn:uuid:{Guid.NewGuid()}"),
                DateTime.Now);

            Assert.Equal(isTransformed, record is WatMetadataRecord);
        }
    }
}
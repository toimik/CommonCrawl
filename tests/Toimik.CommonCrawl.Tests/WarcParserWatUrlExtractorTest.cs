namespace Toimik.CommonCrawl.Tests
{
    using System.Net.Http;
    using Toimik.CommonCrawl;
    using Toimik.WarcProtocol;

    public class WarcParserWatUrlExtractorTest : WatUrlExtractorTest<Record>
    {
        protected override WatUrlExtractor<Record> CreateExtractor(HttpMessageHandler messageHandler, string hostname)
        {
            var streamer = new WarcParserStreamer(
                new HttpClient(messageHandler),
                new WarcParser(new WatRecordFactory(hostname)),
                new ParseLogAdapter());
            return new WarcParserWatUrlExtractor(streamer);
        }
    }
}
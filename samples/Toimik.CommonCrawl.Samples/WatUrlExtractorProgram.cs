namespace Toimik.CommonCrawl.Samples
{
    using System;
    using System.Diagnostics;
    using System.Net.Http;
    using System.Threading.Tasks;
    using Toimik.WarcProtocol;

    public class WatUrlExtractorProgram
    {
        public static async Task Main()
        {
            const string Hostname = "commoncrawl.s3.amazonaws.com";

            // Common Crawl's WAT files start with a warcinfo record. It is observed that the second
            // one is a metadata record of that warcinfo. The problem is that the WARC-Target-URI
            // value of that record uses a relative URL, which is the name of the URL segment.
            //
            // As the WarcProtocol.WarcParser expects an absolute URL, this factory takes care of it
            // by prefixing that URL with the hostname.
            var recordFactory = new WatRecordFactory(Hostname);

            var streamer = new WarcParserStreamer(
                new HttpClient(), // Ideally, a singleton
                new WarcParser(recordFactory),
                new DebugParseLog());
            var extractor = new WarcParserWatUrlExtractor(streamer);

            // The example below uses October 2021's dataset. Other datasets are found at
            // https://commoncrawl.org/the-data/get-started.
            var urlSegmentList = "/crawl-data/CC-MAIN-2021-43/wat.paths.gz";

            var results = extractor.Extract(Hostname, urlSegmentList);
            await foreach (WatUrlExtractor<Record>.Result result in results)
            {
                Console.WriteLine($"{result.Index}: {result.Url}");
            }
        }

        private class DebugParseLog : IParseLog
        {
            public void ChunkSkipped(string chunk)
            {
                Debug.WriteLine(chunk);
            }

            public void ErrorEncountered(string error)
            {
                Debug.WriteLine(error);
            }
        }
    }
}
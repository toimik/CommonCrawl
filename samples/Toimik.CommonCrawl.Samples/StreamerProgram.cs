namespace Toimik.CommonCrawl.Samples;

using System.Diagnostics;
using System.Net.Http;
using System.Threading.Tasks;
using Toimik.WarcProtocol;

public class StreamerProgram
{
    public static async Task Main()
    {
        var streamer = new WarcParserStreamer(
            new HttpClient(), // Ideally a singleton
            new WarcParser(),
            new DebugParseLog());

        // The example below uses October 2021's dataset. Other datasets are found at
        // https://commoncrawl.org/the-data/get-started.
        var urlSegmentList = "/crawl-data/CC-MAIN-2021-43/warc.paths.gz";

        // var urlSegmentList = "/crawl-data/CC-MAIN-2021-43/wat.paths.gz";

        // var urlSegmentList = "/crawl-data/CC-MAIN-2021-43/wet.paths.gz";
        var results = streamer.Stream(hostname: "commoncrawl.s3.amazonaws.com", urlSegmentList);
        await foreach (Streamer<Record>.Result result in results.ConfigureAwait(false))
        {
            var record = result.RecordSegment.Value;

            // The applicable types depend on the selected dataset list path
            switch (record.Type)
            {
                case ConversionRecord.TypeName:

                    // ...
                    break;

                case MetadataRecord.TypeName:

                    // ...
                    break;

                case RequestRecord.TypeName:

                    // ...
                    break;

                case ResponseRecord.TypeName:

                    // ...
                    break;

                case WarcinfoRecord.TypeName:

                    // ...
                    break;
            }
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
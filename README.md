![GitHub Workflow Status](https://img.shields.io/github/workflow/status/toimik/CommonCrawl/CI)
![Code Coverage](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/nurhafiz/66904bd88c3b6c6113fedcfd438fe17c/raw/CommonCrawl-coverage.json)
![Nuget](https://img.shields.io/nuget/v/Toimik.CommonCrawl)

# Toimik.CommonCrawl

.NET 5 C# [Common Crawl](http://commoncrawl.org) processing tools.

## Features

- Parses WARC / WAT / WET datasets via streaming (read: no local download required)
- Extracts URLs from WAT datasets via streaming
- More to come ...

## Quick Start

### Installation

#### Package Manager

```command
PM> Install-Package Toimik.CommonCrawl
```

#### .NET CLI

```command
> dotnet add package Toimik.CommonCrawl
```

### Usage

#### Streaming WARC / WAT / WET datasets

The code below is for streaming from remote datasets.
To process local datasets, use [Toimik.WarcProtocol](https://github.com/toimik/WarcProtocol).

```c# 
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
        await foreach (Streamer<Record>.Result result in results)
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
```
&nbsp;
#### Extracting URLs from streamed WAT datasets

```c# 
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
```
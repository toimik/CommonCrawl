![GitHub Workflow Status](https://img.shields.io/github/workflow/status/toimik/CommonCrawl/CI)
![Code Coverage](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/nurhafiz/66904bd88c3b6c6113fedcfd438fe17c/raw/CommonCrawl-coverage.json)
![Nuget](https://img.shields.io/nuget/v/Toimik.CommonCrawl)

# Toimik.CommonCrawl

.NET 5 C# [Common Crawl](http://commoncrawl.org) processing tools.

## Features

- Parses WARC / WAT / WET datasets via streaming (read: no local download required)
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

```c# 
using System.Diagnostics;
using System.Net.Http;
using System.Threading.Tasks;
using Toimik.CommonCrawl;
using Toimik.WarcProtocol;

class Program
{
    static async Task Main(string[] args)
    {
        var streamer = new WarcParserStreamer(
            new HttpClient(), // Ideally a singleton
            new WarcParser(compressionStreamFactory: new CompressionStreamFactory()),
            new DebugParseLog());

        // The example below uses October 2021's dataset. Other datasets are found at
        // https://commoncrawl.org/the-data/get-started.

        var datasetListPath = "/crawl-data/CC-MAIN-2021-43/warc.paths.gz";

        // var datasetListPath = "/crawl-data/CC-MAIN-2021-43/wat.paths.gz";

        // var datasetListPath = "/crawl-data/CC-MAIN-2021-43/wet.paths.gz";

        var streamResults = streamer.Stream(
            hostname: "commoncrawl.s3.amazonaws.com",
            datasetListPath: datasetListPath);
        await foreach (StreamResult<Record> streamResult in streamResults)
        {
            var record = streamResult.RecordEntry.Value;

            // The applicable types depend on the selected dataset list path
            switch (record.Type.ToLower())
            {
                case "conversion":

                    // ...
                    break;

                case "metadata":

                    // ...
                    break;

                case "request":

                    // ...
                    break;

                case "response":

                    // ...
                    break;

                case "warcinfo":

                    // ...
                    break;
            }
        }
    }

    class DebugParseLog : IParseLog
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
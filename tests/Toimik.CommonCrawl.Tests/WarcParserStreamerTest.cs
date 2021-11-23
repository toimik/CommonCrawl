namespace Toimik.CommonCrawl.Tests
{
    using System;
    using System.Diagnostics.CodeAnalysis;
    using System.IO;
    using System.Linq;
    using System.Net.Http;
    using System.Threading;
    using System.Threading.Tasks;
    using Moq;
    using Moq.Protected;
    using Toimik.WarcProtocol;
    using Xunit;

    // NOTE: index.txt.gz contains these entries (without the dashes):
    // - warcinfo.warc
    // - conversion.warc
    // - metadata.warc
    public class WarcParserStreamerTest
    {
        [Theory]
        [InlineData(0, 0)]
        [InlineData(-1, -1)]
        public async Task StreamFromFirstRecordOfFirstDataset(int datasetStartIndex, int recordStartIndex)
        {
            var messageHandlerMock = new Mock<HttpMessageHandler>();
            using var mainStream = File.OpenRead($"Data{Path.DirectorySeparatorChar}index.txt.gz");
            using var warcinfoStream = File.OpenRead($"Data{Path.DirectorySeparatorChar}warcinfo.warc");
            using var conversionStream = File.OpenRead($"Data{Path.DirectorySeparatorChar}conversion.warc");
            using var metadataStream = File.OpenRead($"Data{Path.DirectorySeparatorChar}metadata.warc");
            _ = messageHandlerMock.Protected()
                .SetupSequence<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(new HttpResponseMessage
                {
                    Content = new StreamContent(mainStream),
                })
                .ReturnsAsync(new HttpResponseMessage
                {
                    Content = new StreamContent(warcinfoStream),
                })
                .ReturnsAsync(new HttpResponseMessage
                {
                    Content = new StreamContent(conversionStream),
                })
                .ReturnsAsync(new HttpResponseMessage
                {
                    Content = new StreamContent(metadataStream),
                });
            var streamer = new WarcParserStreamer(
                new HttpClient(messageHandlerMock.Object),
                new WarcParser(),
                new ParseLog());
            const string Hostname = "www.example.com";

            var streamResults = await streamer.Stream(
                Hostname,
                datasetListPath: "/foobar",
                datasetStartIndex: datasetStartIndex,
                recordStartIndex: recordStartIndex).ToListAsync();

            var streamResult = streamResults[0];
            Assert.Equal($"https://{Hostname}/warcinfo.warc", streamResult.DatasetEntry.Url.ToString());
            Assert.Equal(new Uri("urn:uuid:b92e8444-34cf-472f-a86e-07b7845ecc05"), streamResult.RecordEntry.Value.Id);
            Assert.Equal(0, streamResult.DatasetEntry.Index);
            Assert.Equal(0, streamResult.RecordEntry.Index);

            streamResult = streamResults[1];
            Assert.Equal($"https://{Hostname}/conversion.warc", streamResult.DatasetEntry.Url.ToString());
            Assert.Equal(new Uri("urn:uuid:d92e8444-34cf-472f-a86e-07b7845ecc05"), streamResult.RecordEntry.Value.Id);
            Assert.Equal(1, streamResult.DatasetEntry.Index);
            Assert.Equal(0, streamResult.RecordEntry.Index);

            streamResult = streamResults[2];
            Assert.Equal($"https://{Hostname}/conversion.warc", streamResult.DatasetEntry.Url.ToString());
            Assert.Equal(new Uri("urn:uuid:d02e8444-34cf-472f-a86e-07b7845ecc05"), streamResult.RecordEntry.Value.Id);
            Assert.Equal(1, streamResult.DatasetEntry.Index);
            Assert.Equal(1, streamResult.RecordEntry.Index);

            streamResult = streamResults[3];
            Assert.Equal($"https://{Hostname}/conversion.warc", streamResult.DatasetEntry.Url.ToString());
            Assert.Equal(new Uri("urn:uuid:d12e8444-34cf-472f-a86e-07b7845ecc05"), streamResult.RecordEntry.Value.Id);
            Assert.Equal(1, streamResult.DatasetEntry.Index);
            Assert.Equal(2, streamResult.RecordEntry.Index);

            streamResult = streamResults[4];
            Assert.Equal($"https://{Hostname}/metadata.warc", streamResult.DatasetEntry.Url.ToString());
            Assert.Equal(new Uri("urn:uuid:c92e8444-34cf-472f-a86e-07b7845ecc05"), streamResult.RecordEntry.Value.Id);
            Assert.Equal(2, streamResult.DatasetEntry.Index);
            Assert.Equal(0, streamResult.RecordEntry.Index);
        }

        [Fact]
        public async Task StreamFromFirstRecordOfSecondDataset()
        {
            var messageHandlerMock = new Mock<HttpMessageHandler>();
            using var mainStream = File.OpenRead($"Data{Path.DirectorySeparatorChar}index.txt.gz");
            using var conversionStream = File.OpenRead($"Data{Path.DirectorySeparatorChar}conversion.warc");
            using var metadataStream = File.OpenRead($"Data{Path.DirectorySeparatorChar}metadata.warc");
            _ = messageHandlerMock.Protected()
                .SetupSequence<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(new HttpResponseMessage
                {
                    Content = new StreamContent(mainStream),
                })
                .ReturnsAsync(new HttpResponseMessage
                {
                    Content = new StreamContent(conversionStream),
                })
                .ReturnsAsync(new HttpResponseMessage
                {
                    Content = new StreamContent(metadataStream),
                });
            var streamer = new WarcParserStreamer(
                new HttpClient(messageHandlerMock.Object),
                new WarcParser(),
                new ParseLog());
            const string Hostname = "www.example.com";

            var streamResults = await streamer.Stream(
                Hostname,
                datasetListPath: "/foobar",
                datasetStartIndex: 1).ToListAsync();

            var streamResult = streamResults[0];
            Assert.Equal($"https://{Hostname}/conversion.warc", streamResult.DatasetEntry.Url.ToString());
            Assert.Equal(new Uri("urn:uuid:d92e8444-34cf-472f-a86e-07b7845ecc05"), streamResult.RecordEntry.Value.Id);
            Assert.Equal(1, streamResult.DatasetEntry.Index);
            Assert.Equal(0, streamResult.RecordEntry.Index);

            streamResult = streamResults[1];
            Assert.Equal($"https://{Hostname}/conversion.warc", streamResult.DatasetEntry.Url.ToString());
            Assert.Equal(new Uri("urn:uuid:d02e8444-34cf-472f-a86e-07b7845ecc05"), streamResult.RecordEntry.Value.Id);
            Assert.Equal(1, streamResult.DatasetEntry.Index);
            Assert.Equal(1, streamResult.RecordEntry.Index);

            streamResult = streamResults[2];
            Assert.Equal($"https://{Hostname}/conversion.warc", streamResult.DatasetEntry.Url.ToString());
            Assert.Equal(new Uri("urn:uuid:d12e8444-34cf-472f-a86e-07b7845ecc05"), streamResult.RecordEntry.Value.Id);
            Assert.Equal(1, streamResult.DatasetEntry.Index);
            Assert.Equal(2, streamResult.RecordEntry.Index);

            streamResult = streamResults[3];
            Assert.Equal($"https://{Hostname}/metadata.warc", streamResult.DatasetEntry.Url.ToString());
            Assert.Equal(new Uri("urn:uuid:c92e8444-34cf-472f-a86e-07b7845ecc05"), streamResult.RecordEntry.Value.Id);
            Assert.Equal(2, streamResult.DatasetEntry.Index);
            Assert.Equal(0, streamResult.RecordEntry.Index);
        }

        [Fact]
        public async Task StreamFromFirstRecordOfThirdDataset()
        {
            var messageHandlerMock = new Mock<HttpMessageHandler>();
            using var mainStream = File.OpenRead($"Data{Path.DirectorySeparatorChar}index.txt.gz");
            using var metadataStream = File.OpenRead($"Data{Path.DirectorySeparatorChar}metadata.warc");
            _ = messageHandlerMock.Protected()
                .SetupSequence<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(new HttpResponseMessage
                {
                    Content = new StreamContent(mainStream),
                })
                .ReturnsAsync(new HttpResponseMessage
                {
                    Content = new StreamContent(metadataStream),
                });
            var streamer = new WarcParserStreamer(
                new HttpClient(messageHandlerMock.Object),
                new WarcParser(),
                new ParseLog());
            const string Hostname = "www.example.com";

            var streamResults = await streamer.Stream(
                Hostname,
                datasetListPath: "/foobar",
                datasetStartIndex: 2).ToListAsync();

            var streamResult = streamResults[0];
            Assert.Equal($"https://{Hostname}/metadata.warc", streamResult.DatasetEntry.Url.ToString());
            Assert.Equal(new Uri("urn:uuid:c92e8444-34cf-472f-a86e-07b7845ecc05"), streamResult.RecordEntry.Value.Id);
            Assert.Equal(2, streamResult.DatasetEntry.Index);
            Assert.Equal(0, streamResult.RecordEntry.Index);
        }

        [Fact]
        public async Task StreamFromNonExistentDataset()
        {
            var messageHandlerMock = new Mock<HttpMessageHandler>();
            using var mainStream = File.OpenRead($"Data{Path.DirectorySeparatorChar}index.txt.gz");
            _ = messageHandlerMock.Protected()
                .SetupSequence<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(new HttpResponseMessage
                {
                    Content = new StreamContent(mainStream),
                });
            var streamer = new WarcParserStreamer(
                new HttpClient(messageHandlerMock.Object),
                new WarcParser(),
                new ParseLog());
            const string Hostname = "www.example.com";

            await Assert.ThrowsAsync<ArgumentException>(
                async () =>
                await streamer.Stream(
                    Hostname,
                    datasetListPath: "/foobar",
                    datasetStartIndex: 3).ToListAsync());
        }

        [Fact]
        public async Task StreamFromNonExistentIndexOfThirdDataset()
        {
            var messageHandlerMock = new Mock<HttpMessageHandler>();
            using var mainStream = File.OpenRead($"Data{Path.DirectorySeparatorChar}index.txt.gz");
            using var metadataStream = File.OpenRead($"Data{Path.DirectorySeparatorChar}metadata.warc");
            _ = messageHandlerMock.Protected()
                .SetupSequence<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(new HttpResponseMessage
                {
                    Content = new StreamContent(mainStream),
                })
                .ReturnsAsync(new HttpResponseMessage
                {
                    Content = new StreamContent(metadataStream),
                });
            var streamer = new WarcParserStreamer(
                new HttpClient(messageHandlerMock.Object),
                new WarcParser(),
                new ParseLog());
            const string Hostname = "www.example.com";

            var streamResults = await streamer.Stream(
                Hostname,
                datasetListPath: "/foobar",
                datasetStartIndex: 2,
                recordStartIndex: 1).ToListAsync();

            Assert.Empty(streamResults);
        }

        [Fact]
        public async Task StreamFromSecondRecordOfSecondDataset()
        {
            var messageHandlerMock = new Mock<HttpMessageHandler>();
            using var mainStream = File.OpenRead($"Data{Path.DirectorySeparatorChar}index.txt.gz");
            using var conversionStream = File.OpenRead($"Data{Path.DirectorySeparatorChar}conversion.warc");
            using var metadataStream = File.OpenRead($"Data{Path.DirectorySeparatorChar}metadata.warc");
            _ = messageHandlerMock.Protected()
                .SetupSequence<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(new HttpResponseMessage
                {
                    Content = new StreamContent(mainStream),
                })
                .ReturnsAsync(new HttpResponseMessage
                {
                    Content = new StreamContent(conversionStream),
                })
                .ReturnsAsync(new HttpResponseMessage
                {
                    Content = new StreamContent(metadataStream),
                });
            var streamer = new WarcParserStreamer(
                new HttpClient(messageHandlerMock.Object),
                new WarcParser(),
                new ParseLog());
            const string Hostname = "www.example.com";

            var streamResults = await streamer.Stream(
                Hostname,
                datasetListPath: "/foobar",
                datasetStartIndex: 1,
                recordStartIndex: 1).ToListAsync();

            var streamResult = streamResults[0];
            Assert.Equal($"https://{Hostname}/conversion.warc", streamResult.DatasetEntry.Url.ToString());
            Assert.Equal(new Uri("urn:uuid:d02e8444-34cf-472f-a86e-07b7845ecc05"), streamResult.RecordEntry.Value.Id);
            Assert.Equal(1, streamResult.DatasetEntry.Index);
            Assert.Equal(1, streamResult.RecordEntry.Index);

            streamResult = streamResults[1];
            Assert.Equal($"https://{Hostname}/conversion.warc", streamResult.DatasetEntry.Url.ToString());
            Assert.Equal(new Uri("urn:uuid:d12e8444-34cf-472f-a86e-07b7845ecc05"), streamResult.RecordEntry.Value.Id);
            Assert.Equal(1, streamResult.DatasetEntry.Index);
            Assert.Equal(2, streamResult.RecordEntry.Index);

            streamResult = streamResults[2];
            Assert.Equal($"https://{Hostname}/metadata.warc", streamResult.DatasetEntry.Url.ToString());
            Assert.Equal(new Uri("urn:uuid:c92e8444-34cf-472f-a86e-07b7845ecc05"), streamResult.RecordEntry.Value.Id);
            Assert.Equal(2, streamResult.DatasetEntry.Index);
            Assert.Equal(0, streamResult.RecordEntry.Index);
        }

        [Fact]
        public async Task StreamFromThirdRecordOfSecondDataset()
        {
            var messageHandlerMock = new Mock<HttpMessageHandler>();
            using var mainStream = File.OpenRead($"Data{Path.DirectorySeparatorChar}index.txt.gz");
            using var conversionStream = File.OpenRead($"Data{Path.DirectorySeparatorChar}conversion.warc");
            using var metadataStream = File.OpenRead($"Data{Path.DirectorySeparatorChar}metadata.warc");
            _ = messageHandlerMock.Protected()
                .SetupSequence<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(new HttpResponseMessage
                {
                    Content = new StreamContent(mainStream),
                })
                .ReturnsAsync(new HttpResponseMessage
                {
                    Content = new StreamContent(conversionStream),
                })
                .ReturnsAsync(new HttpResponseMessage
                {
                    Content = new StreamContent(metadataStream),
                });
            var streamer = new WarcParserStreamer(
                new HttpClient(messageHandlerMock.Object),
                new WarcParser(),
                new ParseLog());
            const string Hostname = "www.example.com";

            var streamResults = await streamer.Stream(
                Hostname,
                datasetListPath: "/foobar",
                datasetStartIndex: 1,
                recordStartIndex: 2).ToListAsync();

            var streamResult = streamResults[0];
            Assert.Equal($"https://{Hostname}/conversion.warc", streamResult.DatasetEntry.Url.ToString());
            Assert.Equal(new Uri("urn:uuid:d12e8444-34cf-472f-a86e-07b7845ecc05"), streamResult.RecordEntry.Value.Id);
            Assert.Equal(1, streamResult.DatasetEntry.Index);
            Assert.Equal(2, streamResult.RecordEntry.Index);

            streamResult = streamResults[1];
            Assert.Equal($"https://{Hostname}/metadata.warc", streamResult.DatasetEntry.Url.ToString());
            Assert.Equal(new Uri("urn:uuid:c92e8444-34cf-472f-a86e-07b7845ecc05"), streamResult.RecordEntry.Value.Id);
            Assert.Equal(2, streamResult.DatasetEntry.Index);
            Assert.Equal(0, streamResult.RecordEntry.Index);
        }

        private class ParseLog : IParseLog
        {
            public void ChunkSkipped(string chunk)
            {
                // Do nothing
            }

            [ExcludeFromCodeCoverage]
            public void ErrorEncountered(string error)
            {
                // Do nothing
            }
        }
    }
}

// todo parse log adapter
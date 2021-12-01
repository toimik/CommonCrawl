namespace Toimik.CommonCrawl.Tests
{
    using System;
    using System.IO;
    using System.Linq;
    using System.Net.Http;
    using System.Threading;
    using System.Threading.Tasks;
    using Moq;
    using Moq.Protected;
    using Toimik.WarcProtocol;
    using Xunit;

    /* NOTE: index.txt.gz contains these entries:
     *  warcinfo.warc
     *  conversion.warc
     *  metadata.warc
     */

    public class WarcParserStreamerTest
    {
        private static readonly string DataDirectory = $"Data{Path.DirectorySeparatorChar}WarcParserStreamer{Path.DirectorySeparatorChar}";

        [Theory]
        [InlineData(0, 0)]
        [InlineData(-1, -1)]
        public async Task FromFirstRecordOfFirstUrl(int urlSegmentOffset, int recordSegmentOffset)
        {
            var messageHandlerMock = new Mock<HttpMessageHandler>();
            using var mainStream = File.OpenRead($"{DataDirectory}index.txt.gz");
            using var warcinfoStream = File.OpenRead($"{DataDirectory}warcinfo.warc");
            using var conversionStream = File.OpenRead($"{DataDirectory}conversion.warc");
            using var metadataStream = File.OpenRead($"{DataDirectory}metadata.warc");
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
            var streamer = CreateStreamer(messageHandlerMock.Object);
            const string Hostname = "www.example.com";

            var results = await streamer.Stream(
                Hostname,
                urlSegmentList: "/foobar",
                urlSegmentOffset: urlSegmentOffset,
                recordSegmentOffset: recordSegmentOffset).ToListAsync();

            var streamResult = results[0];
            Assert.Equal($"https://{Hostname}/warcinfo.warc", streamResult.UrlSegment.Value);
            Assert.Equal(new Uri("urn:uuid:b92e8444-34cf-472f-a86e-07b7845ecc05"), streamResult.RecordSegment.Value.Id);
            Assert.Equal(0, streamResult.UrlSegment.Index);
            Assert.Equal(0, streamResult.RecordSegment.Index);

            streamResult = results[1];
            Assert.Equal($"https://{Hostname}/conversion.warc", streamResult.UrlSegment.Value);
            Assert.Equal(new Uri("urn:uuid:d92e8444-34cf-472f-a86e-07b7845ecc05"), streamResult.RecordSegment.Value.Id);
            Assert.Equal(1, streamResult.UrlSegment.Index);
            Assert.Equal(0, streamResult.RecordSegment.Index);

            streamResult = results[2];
            Assert.Equal($"https://{Hostname}/conversion.warc", streamResult.UrlSegment.Value);
            Assert.Equal(new Uri("urn:uuid:d02e8444-34cf-472f-a86e-07b7845ecc05"), streamResult.RecordSegment.Value.Id);
            Assert.Equal(1, streamResult.UrlSegment.Index);
            Assert.Equal(1, streamResult.RecordSegment.Index);

            streamResult = results[3];
            Assert.Equal($"https://{Hostname}/conversion.warc", streamResult.UrlSegment.Value);
            Assert.Equal(new Uri("urn:uuid:d12e8444-34cf-472f-a86e-07b7845ecc05"), streamResult.RecordSegment.Value.Id);
            Assert.Equal(1, streamResult.UrlSegment.Index);
            Assert.Equal(2, streamResult.RecordSegment.Index);

            streamResult = results[4];
            Assert.Equal($"https://{Hostname}/metadata.warc", streamResult.UrlSegment.Value);
            Assert.Equal(new Uri("urn:uuid:c92e8444-34cf-472f-a86e-07b7845ecc05"), streamResult.RecordSegment.Value.Id);
            Assert.Equal(2, streamResult.UrlSegment.Index);
            Assert.Equal(0, streamResult.RecordSegment.Index);
        }

        [Fact]
        public async Task FromFirstRecordOfSecondUrl()
        {
            var messageHandlerMock = new Mock<HttpMessageHandler>();
            using var mainStream = File.OpenRead($"{DataDirectory}index.txt.gz");
            using var conversionStream = File.OpenRead($"{DataDirectory}conversion.warc");
            using var metadataStream = File.OpenRead($"{DataDirectory}metadata.warc");
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
            var streamer = CreateStreamer(messageHandlerMock.Object);
            const string Hostname = "www.example.com";

            var results = await streamer.Stream(
                Hostname,
                urlSegmentList: "/foobar",
                urlSegmentOffset: 1).ToListAsync();

            var streamResult = results[0];
            Assert.Equal($"https://{Hostname}/conversion.warc", streamResult.UrlSegment.Value);
            Assert.Equal(new Uri("urn:uuid:d92e8444-34cf-472f-a86e-07b7845ecc05"), streamResult.RecordSegment.Value.Id);
            Assert.Equal(1, streamResult.UrlSegment.Index);
            Assert.Equal(0, streamResult.RecordSegment.Index);

            streamResult = results[1];
            Assert.Equal($"https://{Hostname}/conversion.warc", streamResult.UrlSegment.Value);
            Assert.Equal(new Uri("urn:uuid:d02e8444-34cf-472f-a86e-07b7845ecc05"), streamResult.RecordSegment.Value.Id);
            Assert.Equal(1, streamResult.UrlSegment.Index);
            Assert.Equal(1, streamResult.RecordSegment.Index);

            streamResult = results[2];
            Assert.Equal($"https://{Hostname}/conversion.warc", streamResult.UrlSegment.Value);
            Assert.Equal(new Uri("urn:uuid:d12e8444-34cf-472f-a86e-07b7845ecc05"), streamResult.RecordSegment.Value.Id);
            Assert.Equal(1, streamResult.UrlSegment.Index);
            Assert.Equal(2, streamResult.RecordSegment.Index);

            streamResult = results[3];
            Assert.Equal($"https://{Hostname}/metadata.warc", streamResult.UrlSegment.Value);
            Assert.Equal(new Uri("urn:uuid:c92e8444-34cf-472f-a86e-07b7845ecc05"), streamResult.RecordSegment.Value.Id);
            Assert.Equal(2, streamResult.UrlSegment.Index);
            Assert.Equal(0, streamResult.RecordSegment.Index);
        }

        [Fact]
        public async Task FromFirstRecordOfThirdUrl()
        {
            var messageHandlerMock = new Mock<HttpMessageHandler>();
            using var mainStream = File.OpenRead($"{DataDirectory}index.txt.gz");
            using var metadataStream = File.OpenRead($"{DataDirectory}metadata.warc");
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
            var streamer = CreateStreamer(messageHandlerMock.Object);
            const string Hostname = "www.example.com";

            var results = await streamer.Stream(
                Hostname,
                urlSegmentList: "/foobar",
                urlSegmentOffset: 2).ToListAsync();

            var streamResult = results[0];
            Assert.Equal($"https://{Hostname}/metadata.warc", streamResult.UrlSegment.Value);
            Assert.Equal(new Uri("urn:uuid:c92e8444-34cf-472f-a86e-07b7845ecc05"), streamResult.RecordSegment.Value.Id);
            Assert.Equal(2, streamResult.UrlSegment.Index);
            Assert.Equal(0, streamResult.RecordSegment.Index);
        }

        [Fact]
        public async Task FromNonExistentOffsetOfThirdUrl()
        {
            var messageHandlerMock = new Mock<HttpMessageHandler>();
            using var mainStream = File.OpenRead($"{DataDirectory}index.txt.gz");
            using var metadataStream = File.OpenRead($"{DataDirectory}metadata.warc");
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
            var streamer = CreateStreamer(messageHandlerMock.Object);

            var results = await streamer.Stream(
                hostname: "www.example.com",
                urlSegmentList: "/foobar",
                urlSegmentOffset: 2,
                recordSegmentOffset: 1).ToListAsync();

            Assert.Empty(results);
        }

        [Fact]
        public async Task FromNonExistentUrl()
        {
            var messageHandlerMock = new Mock<HttpMessageHandler>();
            using var mainStream = File.OpenRead($"{DataDirectory}index.txt.gz");
            _ = messageHandlerMock.Protected()
                .SetupSequence<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(new HttpResponseMessage
                {
                    Content = new StreamContent(mainStream),
                });
            var streamer = CreateStreamer(messageHandlerMock.Object);

            await Assert.ThrowsAsync<ArgumentException>(
                async () =>
                await streamer.Stream(
                    hostname: "www.example.com",
                    urlSegmentList: "/foobar",
                    urlSegmentOffset: 3).ToListAsync());
        }

        [Fact]
        public async Task FromSecondRecordOfSecondUrl()
        {
            var messageHandlerMock = new Mock<HttpMessageHandler>();
            using var mainStream = File.OpenRead($"{DataDirectory}index.txt.gz");
            using var conversionStream = File.OpenRead($"{DataDirectory}conversion.warc");
            using var metadataStream = File.OpenRead($"{DataDirectory}metadata.warc");
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
            var streamer = CreateStreamer(messageHandlerMock.Object);
            const string Hostname = "www.example.com";

            var results = await streamer.Stream(
                Hostname,
                urlSegmentList: "/foobar",
                urlSegmentOffset: 1,
                recordSegmentOffset: 1).ToListAsync();

            var streamResult = results[0];
            Assert.Equal($"https://{Hostname}/conversion.warc", streamResult.UrlSegment.Value);
            Assert.Equal(new Uri("urn:uuid:d02e8444-34cf-472f-a86e-07b7845ecc05"), streamResult.RecordSegment.Value.Id);
            Assert.Equal(1, streamResult.UrlSegment.Index);
            Assert.Equal(1, streamResult.RecordSegment.Index);

            streamResult = results[1];
            Assert.Equal($"https://{Hostname}/conversion.warc", streamResult.UrlSegment.Value);
            Assert.Equal(new Uri("urn:uuid:d12e8444-34cf-472f-a86e-07b7845ecc05"), streamResult.RecordSegment.Value.Id);
            Assert.Equal(1, streamResult.UrlSegment.Index);
            Assert.Equal(2, streamResult.RecordSegment.Index);

            streamResult = results[2];
            Assert.Equal($"https://{Hostname}/metadata.warc", streamResult.UrlSegment.Value);
            Assert.Equal(new Uri("urn:uuid:c92e8444-34cf-472f-a86e-07b7845ecc05"), streamResult.RecordSegment.Value.Id);
            Assert.Equal(2, streamResult.UrlSegment.Index);
            Assert.Equal(0, streamResult.RecordSegment.Index);
        }

        [Fact]
        public async Task FromThirdRecordOfSecondUrl()
        {
            var messageHandlerMock = new Mock<HttpMessageHandler>();
            using var mainStream = File.OpenRead($"{DataDirectory}index.txt.gz");
            using var conversionStream = File.OpenRead($"{DataDirectory}conversion.warc");
            using var metadataStream = File.OpenRead($"{DataDirectory}metadata.warc");
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
            var streamer = CreateStreamer(messageHandlerMock.Object);
            const string Hostname = "www.example.com";

            var results = await streamer.Stream(
                Hostname,
                urlSegmentList: "/foobar",
                urlSegmentOffset: 1,
                recordSegmentOffset: 2).ToListAsync();

            var streamResult = results[0];
            Assert.Equal($"https://{Hostname}/conversion.warc", streamResult.UrlSegment.Value);
            Assert.Equal(new Uri("urn:uuid:d12e8444-34cf-472f-a86e-07b7845ecc05"), streamResult.RecordSegment.Value.Id);
            Assert.Equal(1, streamResult.UrlSegment.Index);
            Assert.Equal(2, streamResult.RecordSegment.Index);

            streamResult = results[1];
            Assert.Equal($"https://{Hostname}/metadata.warc", streamResult.UrlSegment.Value);
            Assert.Equal(new Uri("urn:uuid:c92e8444-34cf-472f-a86e-07b7845ecc05"), streamResult.RecordSegment.Value.Id);
            Assert.Equal(2, streamResult.UrlSegment.Index);
            Assert.Equal(0, streamResult.RecordSegment.Index);
        }

        private static WarcParserStreamer CreateStreamer(HttpMessageHandler messageHandler)
        {
            return new WarcParserStreamer(
                new HttpClient(messageHandler),
                new WarcParser(),
                new ParseLogAdapter());
        }
    }
}
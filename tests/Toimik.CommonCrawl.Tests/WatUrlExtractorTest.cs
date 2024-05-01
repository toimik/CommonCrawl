namespace Toimik.CommonCrawl.Tests;

using Moq;
using Moq.Protected;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

/* NOTE: Only offsets are tested because the other arguments are already covered by WarcParserStreamerTest */

public abstract class WatUrlExtractorTest<T>
{
    /* NOTE: index.txt.gz contains these entries:
     * metadata.warc
     * metadata2.warc
     * metadata3.warc
     */

    private static readonly string DataDirectory = $"Data{Path.DirectorySeparatorChar}WarcParserWatUrlExtractor{Path.DirectorySeparatorChar}";

    private static readonly string DataDirectoryForOptional = $"Data{Path.DirectorySeparatorChar}WarcParserWatUrlExtractor{Path.DirectorySeparatorChar}Optional{Path.DirectorySeparatorChar}";

    [Fact]
    public async Task InvalidJsonContentBlock()
    {
        // NOTE: index.txt.gz contains one entry that does not point to an actual file: foobar
        var messageHandlerMock = new Mock<HttpMessageHandler>();
        var directory = $"Data{Path.DirectorySeparatorChar}WarcParserWatUrlExtractor{Path.DirectorySeparatorChar}Invalid{Path.DirectorySeparatorChar}";
        using var mainStream = File.OpenRead($"{directory}index.txt.gz");
        using var miscStream = File.OpenRead($"{directory}invalid.warc");
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
                Content = new StreamContent(miscStream),
            });
        const string Hostname = "www.example.com";
        var extractor = CreateExtractor(messageHandlerMock.Object, Hostname);

        var results = await extractor.Extract(Hostname, urlSegmentList: "/foobar").ToListAsync();

        Assert.Empty(results);
    }

    [Fact]
    public async Task OffsetToBeyond()
    {
        var messageHandlerMock = new Mock<HttpMessageHandler>();
        using var mainStream = File.OpenRead($"{DataDirectory}index.txt.gz");
        using var metadataStream = File.OpenRead($"{DataDirectory}metadata.warc");
        using var metadata2Stream = File.OpenRead($"{DataDirectory}metadata2.warc");
        using var metadata3Stream = File.OpenRead($"{DataDirectory}metadata3.warc");
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
            })
            .ReturnsAsync(new HttpResponseMessage
            {
                Content = new StreamContent(metadata2Stream),
            })
            .ReturnsAsync(new HttpResponseMessage
            {
                Content = new StreamContent(metadata3Stream),
            });
        const string Hostname = "www.example.com";
        var extractor = CreateExtractor(messageHandlerMock.Object, Hostname);

        var entryOffset = 9;
        var results = await extractor.Extract(
            Hostname,
            urlSegmentList: "/foobar",
            entryOffset: entryOffset).ToListAsync();

        Assert.Empty(results);
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(0)]
    public async Task OffsetToFirst(int entryOffset)
    {
        var messageHandlerMock = new Mock<HttpMessageHandler>();
        using var mainStream = File.OpenRead($"{DataDirectory}index.txt.gz");
        using var metadataStream = File.OpenRead($"{DataDirectory}metadata.warc");
        using var metadata2Stream = File.OpenRead($"{DataDirectory}metadata2.warc");
        using var metadata3Stream = File.OpenRead($"{DataDirectory}metadata3.warc");
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
            })
            .ReturnsAsync(new HttpResponseMessage
            {
                Content = new StreamContent(metadata2Stream),
            })
            .ReturnsAsync(new HttpResponseMessage
            {
                Content = new StreamContent(metadata3Stream),
            });
        const string Hostname = "www.example.com";
        var extractor = CreateExtractor(messageHandlerMock.Object, Hostname);
        var expectedUrls = new List<string>
        {
            // metadata.warc is read but does not yield any URLs because the WARC-Target-Uri is relative

            // From metadata2.warc
            "http://www.example.com",
            $"http://{Hostname}/foo.css",
            $"http://{Hostname}/foobar.css",
            "https://www.example.com/foo.css",
            "http://www.example.com/foobar.js",
            "http://www.example.sg/foobar.js",
            "http://www.example.com/3",
            "http://www.example.com/foo_bar.js",
        };

        var results = await extractor.Extract(
            Hostname,
            urlSegmentList: "/foobar",
            entryOffset: entryOffset).ToListAsync();

        for (int i = 0; i < results.Count; i++)
        {
            var result = results[i];
            Assert.Equal(i, result.Index);
            var expectedUrl = expectedUrls[i];
            Assert.Equal(expectedUrl, result.Url);
            i++;
        }
    }

    [Fact]
    public async Task OffsetToLast()
    {
        var messageHandlerMock = new Mock<HttpMessageHandler>();
        using var mainStream = File.OpenRead($"{DataDirectory}index.txt.gz");
        using var metadataStream = File.OpenRead($"{DataDirectory}metadata.warc");
        using var metadata2Stream = File.OpenRead($"{DataDirectory}metadata2.warc");
        using var metadata3Stream = File.OpenRead($"{DataDirectory}metadata3.warc");
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
            })
            .ReturnsAsync(new HttpResponseMessage
            {
                Content = new StreamContent(metadata2Stream),
            })
            .ReturnsAsync(new HttpResponseMessage
            {
                Content = new StreamContent(metadata3Stream),
            });
        const string Hostname = "www.example.com";
        var extractor = CreateExtractor(messageHandlerMock.Object, Hostname);

        // The offset does not take into account the one URL in metadata.warc because that URL is
        // skipped as it is a relative one
        var entryOffset = 7;
        var results = await extractor.Extract(
            Hostname,
            urlSegmentList: "/foobar",
            entryOffset: entryOffset).ToListAsync();

        var result = results[0];
        Assert.Equal(entryOffset, result.Index);
        Assert.Equal("http://www.example.com/foo_bar.js", result.Url);
    }

    [Theory]
    [InlineData("head.warc")]
    [InlineData("html-metadata.warc")]
    [InlineData("http-response-metadata.warc")]
    [InlineData("missing.warc")]
    [InlineData("payload-metadata.warc")]
    public async Task WithoutMetadata(string filename)
    {
        // NOTE: index.txt.gz contains one entry that does not point to an actual file: foobar
        var messageHandlerMock = new Mock<HttpMessageHandler>();
        using var mainStream = File.OpenRead($"{DataDirectoryForOptional}index.txt.gz");
        using var optionalStream = File.OpenRead($"{DataDirectoryForOptional}{filename}");
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
                Content = new StreamContent(optionalStream),
            });
        const string Hostname = "www.example.com";
        var extractor = CreateExtractor(messageHandlerMock.Object, Hostname);

        var results = await extractor.Extract(Hostname, urlSegmentList: "/foobar").ToListAsync();

        var result = results[0];
        Assert.Equal(0, result.Index);
        Assert.Equal("http://www.example.com/", result.Url);
    }

    [Fact]
    public async Task WithoutTargetUri()
    {
        var messageHandlerMock = new Mock<HttpMessageHandler>();
        using var mainStream = File.OpenRead($"{DataDirectoryForOptional}index.txt.gz");
        using var optionalStream = File.OpenRead($"{DataDirectoryForOptional}warc-target-uri.warc");
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
                Content = new StreamContent(optionalStream),
            });
        const string Hostname = "www.example.com";
        var extractor = CreateExtractor(messageHandlerMock.Object, Hostname);

        var results = await extractor.Extract(Hostname, urlSegmentList: "/foobar").ToListAsync();

        Assert.Empty(results);
    }

    protected abstract WatUrlExtractor<T> CreateExtractor(HttpMessageHandler messageHandler, string hostname);
}
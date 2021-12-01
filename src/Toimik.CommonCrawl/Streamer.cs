/*
 * Copyright 2021 nurhafiz@hotmail.sg
 *
 * Licensed under the Apache License, version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Toimik.CommonCrawl
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Net.Http;
    using System.Runtime.CompilerServices;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Represents a class that streams the Common Crawl - www.commoncrawl.org - crawl archive.
    /// </summary>
    /// <typeparam name="T">
    /// The generic type.
    /// </typeparam>
    /// <remarks>
    /// Common Crawl provides free monthly data dump of their web crawl. Each file is large and the
    /// cumulative size adds up to petabytes. As such, it is more practical and time-saving to
    /// process those files via streaming instead of downloading them first.
    /// </remarks>
    public abstract class Streamer<T>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="Streamer{T}"/> class that streams over
        /// HTTPS.
        /// </summary>
        /// <param name="httpClient">
        /// The <see cref="HttpClient"/> used to make HTTP requests.
        /// </param>
        protected Streamer(HttpClient httpClient)
        {
            HttpClient = httpClient;
        }

        public HttpClient HttpClient { get; }

        /// <summary>
        /// Streams, for this instance, the records found in every datasets hosted at the specified
        /// HTTPS location.
        /// </summary>
        /// <param name="hostname">
        /// The hostname where the datasets are located. e.g. <c>commoncrawl.s3.amazonaws.com</c>.
        /// </param>
        /// <param name="urlSegmentList">
        /// The case-sensitive path where the URL segment list is located. e.g.
        /// <c>/crawl-data/CC-MAIN-YYYY-WW/[warc|wat|wet].paths.gz</c>.
        /// </param>
        /// <param name="urlSegmentOffset">
        /// The zero-based offset of the URL segment to start processing from. This is useful to
        /// continue from a previous stream. If this is negative, it defaults to <c>0</c>. The
        /// default is <c>0</c>.
        /// </param>
        /// <param name="recordSegmentOffset">
        /// The zero-based offset of the record segment to start processing from. This is useful to
        /// continue from a previous stream. If this is negative, it defaults to <c>0</c>. The
        /// default is <c>0</c>.
        /// </param>
        /// <param name="cancellationToken">
        /// Optional token to monitor for cancellation request.
        /// </param>
        /// <returns>
        /// Enumerable of <see cref="Result"/>.
        /// </returns>
        /// <remarks>
        /// The file at <c>https://[ <paramref name="hostname"/> ][
        /// <paramref name="urlSegmentList"/> ]</c> contains one <c>path</c> per line. When formed
        /// into a URL - <c>https://[ <paramref name="hostname"/> ][path]</c>, each one points to a
        /// dataset.
        /// </remarks>
        // NOTE: Parallelism is not built-in so as to not overload the Common Crawl server, which
        // has limited resources due to its non-profit nature
        public async IAsyncEnumerable<Result> Stream(
            string hostname,
            string urlSegmentList,
            int urlSegmentOffset = 0,
            int recordSegmentOffset = 0,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            var urlSegment = $"https://{hostname.ToLower()}{urlSegmentList}";
            using var stream = await Connect(urlSegment, cancellationToken);

            // As the file is guaranteed to be compressed, it needs to be decompressed
            using var decompressedStream = Decompress(stream);

            using var reader = new StreamReader(decompressedStream);

            // Skip, if any, segments that were streamed
            int index;
            string dataUrl;
            if (urlSegmentOffset == 0)
            {
                index = 0;
                dataUrl = await reader.ReadLineAsync();
            }
            else
            {
                if (urlSegmentOffset < 0)
                {
                    urlSegmentOffset = 0;
                }

                for (index = 0; index < urlSegmentOffset; index++)
                {
                    await reader.ReadLineAsync();
                }

                dataUrl = await reader.ReadLineAsync();
                if (dataUrl == null)
                {
                    throw new ArgumentException($"Invalid {nameof(urlSegmentOffset)}.");
                }
            }

            dataUrl = $"https://{hostname}/{dataUrl}";

            // Skip, if any, records that were streamed
            var results = Stream(new Segment<string>(index, dataUrl), cancellationToken);

            index++;
            if (recordSegmentOffset < 0)
            {
                recordSegmentOffset = 0;
            }

            results = results.Skip(recordSegmentOffset);

            await foreach (Result result in results)
            {
                yield return result;
            }

            // Stream the rest of the records from the rest of the segments
            string line;
            while ((line = await reader.ReadLineAsync()) != null)
            {
                dataUrl = $"https://{hostname}/{line}";
                await foreach (Result result in Stream(new Segment<string>(index, dataUrl), cancellationToken))
                {
                    yield return result;
                }

                index++;
            }

            yield break;
        }

        protected async Task<Stream> Connect(string url, CancellationToken cancellationToken)
        {
            var requestMessage = new HttpRequestMessage(HttpMethod.Get, url);
            var responseMessage = await HttpClient.SendAsync(
                requestMessage,
                HttpCompletionOption.ResponseHeadersRead,
                cancellationToken);
            var stream = await responseMessage.Content.ReadAsStreamAsync(cancellationToken);
            return stream;
        }

        protected abstract Stream Decompress(Stream stream);

        protected abstract IAsyncEnumerable<Result> Stream(Segment<string> urlSegment, CancellationToken cancellationToken);

        public struct Result
        {
            public Result(Segment<string> urlSegment, Segment<T> recordSegment)
            {
                UrlSegment = urlSegment;
                RecordSegment = recordSegment;
            }

            public Segment<T> RecordSegment { get; }

            public Segment<string> UrlSegment { get; }
        }
    }
}
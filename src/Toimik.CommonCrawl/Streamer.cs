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
    using System.Diagnostics.CodeAnalysis;
    using System.IO;
    using System.Linq;
    using System.Net.Http;
    using System.Runtime.CompilerServices;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Represents a class that streams the Common Crawl - www.commoncrawl.org - crawl archive.
    /// </summary>
    /// <remarks>
    /// Common Crawl provides free monthly data dump of their web crawl. Each file is large and the
    /// cumulative size adds up to petabytes. As such, it is more practical and time-saving to
    /// process those files via streaming instead of downloading them first.
    /// </remarks>
    public abstract class Streamer<T> : IDisposable
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="Streamer"/> class that streams over HTTPS.
        /// </summary>
        /// <param name="httpMessageHandler">
        /// Message handler used by the internal <see cref="HttpClient"/>.
        /// </param>
        protected Streamer(HttpMessageHandler httpMessageHandler)
        {
            HttpClient = new HttpClient(httpMessageHandler);
        }

        protected HttpClient HttpClient { get; private set; }

        [ExcludeFromCodeCoverage]
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Streams the records found in every dataset listed in the specified HTTPS location.
        /// </summary>
        /// <param name="hostname">
        /// The hostname where datasets are located. e.g. <c>commoncrawl.s3.amazonaws.com</c>
        /// </param>
        /// <param name="datasetListPath">
        /// The case-sensitive path where the dataset list is located. e.g.
        /// <c>/crawl-data/CC-MAIN-YYYY-WW/[warc|wat|wet].paths.gz</c>
        /// </param>
        /// <param name="datasetStartIndex">
        /// The zero-based index of the dataset entry to start processing from. If this is
        /// <c>null</c>, processing starts from the first entry. This is useful to continue from a
        /// previous stream. If this is negative, it defaults to <c>0</c>.
        /// </param>
        /// <param name="recordStartIndex">
        /// The zero-based index of the record entry to start processing from. If this is
        /// <c>null</c>, processing starts from the first entry. This is useful to continue from a
        /// previous stream. If this is negative, it defaults to <c>0</c>.
        /// </param>
        /// <param name="cancellationToken">
        /// Optional token to monitor for cancellation request.
        /// </param>
        /// <returns>
        /// Enumerable of <see cref="StreamResult"/>.
        /// </returns>
        /// <remarks>
        /// The file at <c>https://[ <paramref name="hostname"/> ][
        /// <paramref name="datasetListPath"/> ]</c> contains one path per line. When formed into a
        /// URL - <c>https://[ <paramref name="hostname"/> ][path]</c>, each one points to a
        /// dataset.
        /// </remarks>
        // NOTE: Parallelism is not built-in so as to not overload the Common Crawl server, which
        // has limited resources due to its non-profit nature
        public async IAsyncEnumerable<StreamResult<T>> Stream(
            string hostname,
            string datasetListPath,
            int? datasetStartIndex = null,
            int? recordStartIndex = null,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            var datasetListUrl = $"https://{hostname.ToLower()}{datasetListPath}";
            using var stream = await Connect(datasetListUrl, cancellationToken);

            // As the file is guaranteed to be compressed, it needs to be decompressed
            using var decompressedStream = Decompress(stream);

            using var reader = new StreamReader(decompressedStream);

            // Skip, if any, datasets that were streamed
            int index;
            string dataUrl;
            if (datasetStartIndex == null)
            {
                index = 0;
                dataUrl = await reader.ReadLineAsync();
            }
            else
            {
                if (datasetStartIndex.Value < 0)
                {
                    datasetStartIndex = 0;
                }

                for (index = 0; index < datasetStartIndex.Value; index++)
                {
                    await reader.ReadLineAsync();
                }

                dataUrl = await reader.ReadLineAsync();
                if (dataUrl == null)
                {
                    throw new ArgumentException($"Invalid {nameof(datasetStartIndex)}.");
                }
            }

            dataUrl = $"https://{hostname}/{dataUrl}";

            // Skip, if any, records that were streamed
            var streamResults = Stream(new DatasetEntry(index, dataUrl), cancellationToken);

            index++;
            if (recordStartIndex != null)
            {
                if (recordStartIndex.Value < 0)
                {
                    recordStartIndex = 0;
                }

                streamResults = streamResults.Skip(recordStartIndex.Value);
            }

            await foreach (StreamResult<T> streamResult in streamResults)
            {
                yield return streamResult;
            }

            // Stream the rest of the records from the rest of the datasets
            string line;
            while ((line = await reader.ReadLineAsync()) != null)
            {
                dataUrl = $"https://{hostname}/{line}";
                await foreach (StreamResult<T> streamResult in Stream(new DatasetEntry(index, dataUrl), cancellationToken))
                {
                    yield return streamResult;
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

        [ExcludeFromCodeCoverage]
        protected virtual void Dispose(bool isDisposing)
        {
            if (isDisposing
                && HttpClient != null)
            {
                HttpClient.Dispose();
                HttpClient = null;
            }
        }

        protected abstract IAsyncEnumerable<StreamResult<T>> Stream(DatasetEntry datasetEntry, CancellationToken cancellationToken);
    }
}
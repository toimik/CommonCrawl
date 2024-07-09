/*
 * Copyright 2021-2024 nurhafiz@hotmail.sg
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

namespace Toimik.CommonCrawl;

using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;

public abstract class WatUrlExtractor<T>(Streamer<T> streamer)
{
    public Streamer<T> Streamer { get; } = streamer;

    // NOTE: See Streamer.Stream(...) for the documentation of most of the arguments
    public async IAsyncEnumerable<Result> Extract(
        string hostname,
        string urlSegmentList,
        int urlSegmentOffset = 0,
        int recordSegmentOffset = 0,
        int entryOffset = 0,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var resultEnumerator = Streamer.Stream(
            hostname,
            urlSegmentList,
            urlSegmentOffset,
            recordSegmentOffset,
            cancellationToken).GetAsyncEnumerator(cancellationToken);
        if (entryOffset < 0)
        {
            entryOffset = 0;
        }

        var index = 0;

        // Skip, if any, the URLs before the offset
        if (entryOffset > 0)
        {
            IEnumerator<string>? urlEnumerator = null;
            while (await resultEnumerator.MoveNextAsync(cancellationToken).ConfigureAwait(false))
            {
                var result = resultEnumerator.Current;
                var urls = ExtractUrls(result);
                urlEnumerator = urls.GetEnumerator();
                while (true)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var hasNext = urlEnumerator.MoveNext();
                    if (!hasNext)
                    {
                        urlEnumerator = null;
                        break;
                    }

                    if (index == entryOffset)
                    {
                        break;
                    }

                    index++;
                }
            }

            // Yield, if any, the URLs that are in the same result but starting from the offset
            if (urlEnumerator != null)
            {
                do
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var url = urlEnumerator.Current;
                    yield return new Result(index, url);
                    index++;
                }
                while (urlEnumerator.MoveNext());
            }
        }

        // Yield, if any, the rest of the URLs
        while (await resultEnumerator.MoveNextAsync(cancellationToken).ConfigureAwait(false))
        {
            var result = resultEnumerator.Current;
            var urls = ExtractUrls(result);
            foreach (string url in urls)
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return new Result(index, url);
                index++;
            }
        }
    }

    protected abstract IEnumerable<string> ExtractUrls(Streamer<T>.Result result);

    public readonly struct Result(int index, string url)
    {
        public int Index { get; } = index;

        /// <summary>
        /// Gets, for this instance, the URL.
        /// </summary>
        /// <remarks>The value may be absolute, comes without a scheme, or comes with any scheme.</remarks>
        public string Url { get; } = url;
    }
}
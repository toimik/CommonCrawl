﻿/*
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
    using System.Collections.Generic;
    using System.IO;
    using System.Net.Http;
    using System.Runtime.CompilerServices;
    using System.Threading;
    using Toimik.WarcProtocol;

    /// <summary>
    /// Represents a <see cref="Streamer{T}"/> that uses <see cref="WarcParser"/> from the
    /// <c>Toimik.WarcProtocol</c> nuget package.
    /// </summary>
    public class WarcParserStreamer : Streamer<Record>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="WarcParserStreamer"/>.
        /// </summary>
        /// <param name="httpClient">
        /// Reference for <see cref="Streamer{T}.HttpClient"/>.
        /// </param>
        /// <param name="parser">
        /// Reference for <see cref="Parser"/>.
        /// </param>
        /// <param name="parseLog">
        /// Reference for <see cref="ParseLog"/>.
        /// </param>
        public WarcParserStreamer(
            HttpClient httpClient,
            WarcParser parser,
            IParseLog parseLog)
            : base(httpClient)
        {
            Parser = parser;
            ParseLog = parseLog;
        }

        /// <summary>
        /// Gets, for this instance, the <see cref="IParseLog"/> used to consume all errors and / or
        /// skipped chunks when parsing the datasets. If this is <c>null</c>, streaming terminates
        /// on the first parsing error.
        /// </summary>
        public IParseLog ParseLog { get; }

        /// <summary>
        /// Gets, for this instance, the <see cref="WarcParser"/> used to parse records from the
        /// datasets.
        /// </summary>
        public WarcParser Parser { get; }

        protected override Stream Decompress(Stream stream)
        {
            return Parser.CompressionStreamFactory.CreateDecompressStream(stream);
        }

        protected override async IAsyncEnumerable<StreamResult<Record>> Stream(DatasetEntry datasetEntry, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            var url = datasetEntry.Url;
            using var stream = await Connect(url, cancellationToken);
            var records = Parser.Parse(
                stream,
                isCompressed: url.EndsWith(".gz"),
                ParseLog,
                byteOffset: 0,
                cancellationToken);
            var index = 0;
            await foreach (Record record in records)
            {
                yield return new StreamResult<Record>(datasetEntry, new RecordEntry<Record>(index++, record));
            }
        }
    }
}
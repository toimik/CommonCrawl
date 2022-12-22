/*
 * Copyright 2021-2022 nurhafiz@hotmail.sg
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
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

using System;
using Toimik.WarcProtocol;

/// <summary>
/// Represents a customized <see cref="MetadataRecord"/> class for Common Crawl's WAT datasets.
/// </summary>
/// <remarks>
/// This is needed because the first record of the WAT dataset uses a relative URL for its
/// <c>WARC-Target-URI</c> value. That is incompatible with what <see cref="WarcParser"/>
/// expects.
/// </remarks>
public class WatMetadataRecord : MetadataRecord
{
    public WatMetadataRecord(
        string version,
        Uri recordId,
        DateTime date,
        DigestFactory digestFactory,
        Uri baseUrl)
        : base(
              version,
              recordId,
              date,
              digestFactory: digestFactory)
    {
        BaseUrl = baseUrl;
    }

    public Uri BaseUrl { get; }

    protected override void Set(string field, string value)
    {
        if (field.Equals(FieldForTargetUri, StringComparison.OrdinalIgnoreCase))
        {
            value = WarcParserWatUrlExtractor.CreateAbsoluteUrl(BaseUrl, value);
        }

        base.Set(field, value);
    }
}
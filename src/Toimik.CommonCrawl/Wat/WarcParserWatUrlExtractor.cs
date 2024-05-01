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

using System;
using System.Collections.Generic;
using System.Text.Json;
using Toimik.WarcProtocol;

public class WarcParserWatUrlExtractor(WarcParserStreamer streamer) : WatUrlExtractor<Record>(streamer)
{
    private static readonly JsonDocumentOptions JsonOptions = new()
    {
        AllowTrailingCommas = true,
    };

    internal static string CreateAbsoluteUrl(Uri baseUrl, string suffix)
    {
        var isAbsolute = Uri.TryCreate(
            suffix,
            UriKind.Absolute,
            out _);
        string url;
        if (isAbsolute)
        {
            url = suffix;
        }
        else
        {
            // Remove leading slash, if any
            var index = suffix.StartsWith('/')
                ? 1
                : 0;
            url = $"{baseUrl}{suffix[index..]}";
        }

        return url;
    }

    protected override IEnumerable<string> ExtractUrls(Streamer<Record>.Result result)
    {
        var record = result.RecordSegment.Value;
        if (record.Type.Equals(MetadataRecord.TypeName))
        {
            var metadataRecord = (MetadataRecord)record;
            var contentBlock = metadataRecord.ContentBlock;
            if (contentBlock == null)
            {
                yield break;
            }

            JsonDocument document;
            try
            {
                document = JsonDocument.Parse(contentBlock, JsonOptions);
            }
            catch (JsonException)
            {
                // Skip if the content block cannot be parsed
                yield break;
            }

            var root = document.RootElement;
            var envelope = root.GetProperty("Envelope");
            var headerMetadata = envelope.GetProperty("WARC-Header-Metadata");

            // Extract, if any, from the WARC-Target-URI
            var hasWarcTargetUri = headerMetadata.TryGetProperty("WARC-Target-URI", out JsonElement warcTargetUri);
            if (!hasWarcTargetUri)
            {
                yield break;
            }

            var targetUri = warcTargetUri.GetString()!;
            var targetUrl = new Uri(targetUri, UriKind.RelativeOrAbsolute);
            if (!targetUrl.IsAbsoluteUri)
            {
                yield break;
            }

            yield return targetUri;

            var baseUrl = new Uri(targetUrl.GetLeftPart(UriPartial.Authority));
            var hasPayloadMetadata = envelope.TryGetProperty("Payload-Metadata", out JsonElement payloadMetadata);
            if (!hasPayloadMetadata)
            {
                yield break;
            }

            var hasResponseMetadata = payloadMetadata.TryGetProperty("HTTP-Response-Metadata", out JsonElement responseMetadata);
            if (!hasResponseMetadata)
            {
                yield break;
            }

            var hasHtmlMetadata = responseMetadata.TryGetProperty("HTML-Metadata", out JsonElement htmlMetadata);
            if (!hasHtmlMetadata)
            {
                yield break;
            }

            // Extract, if any, URLs in body
            var urls = ExtractUrls(
                baseUrl,
                htmlMetadata,
                childName: "Links");
            foreach (string url in urls)
            {
                yield return url;
            }

            var hasHead = htmlMetadata.TryGetProperty("Head", out JsonElement head);
            if (!hasHead)
            {
                yield break;
            }

            // Extract, if any, URLs in CSS' link tags
            urls = ExtractUrls(
                baseUrl,
                head,
                childName: "Link");
            foreach (string url in urls)
            {
                yield return url;
            }

            // Extract, if any, URLs in script tags
            urls = ExtractUrls(
                baseUrl,
                head,
                childName: "Scripts");
            foreach (string url in urls)
            {
                yield return url;
            }
        }
    }

    private static IEnumerable<string> ExtractUrls(
        Uri baseUrl,
        JsonElement parent,
        string childName)
    {
        var hasChild = parent.TryGetProperty(childName, out JsonElement children);
        if (!hasChild)
        {
            yield break;
        }

        foreach (JsonElement child in children.EnumerateArray())
        {
            // NOTE: It's been observed that either "href" or "url" is used
            var hasKey = child.TryGetProperty("url", out JsonElement value);
            if (!hasKey)
            {
                hasKey = child.TryGetProperty("href", out value);
                if (!hasKey)
                {
                    // Play it safe in case both don't exist
                    yield break;
                }
            }

            var url = CreateAbsoluteUrl(baseUrl, value.GetString()!);
            yield return url;
        }
    }
}
/*
 * Copyright 2021-2024 nurhafiz@hotmail.sg
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

public class WatRecordFactory(
    string hostname,
    DigestFactory? digestFactory = null,
    PayloadTypeIdentifier? payloadTypeIdentifier = null) : RecordFactory(
        digestFactory,
        payloadTypeIdentifier)
{
    private bool isFirstMetadataRecord = true;

    public string Hostname { get; } = hostname;

    public override Record CreateRecord(
        string version,
        string recordType,
        Uri recordId,
        DateTime date)
    {
        Record record;
        switch (recordType.ToLower())
        {
            case MetadataRecord.TypeName:
                if (!isFirstMetadataRecord)
                {
                    record = base.CreateRecord(
                        version,
                        recordType,
                        recordId,
                        date);
                }
                else
                {
                    // The first metadata record may be problematic because its WARC-Target-URI uses
                    // a relative URL. This class takes care of that.
                    record = new WatMetadataRecord(
                        version,
                        recordId,
                        date,
                        DigestFactory,
                        new Uri($"https://{Hostname}"));
                    isFirstMetadataRecord = false;
                }

                break;

            default:
                record = base.CreateRecord(
                    version,
                    recordType,
                    recordId,
                    date);
                break;
        }

        return record;
    }
}
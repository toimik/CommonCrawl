namespace Toimik.CommonCrawl.Tests;

using System.Diagnostics.CodeAnalysis;
using Toimik.WarcProtocol;

public class ParseLogAdapter : IParseLog
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
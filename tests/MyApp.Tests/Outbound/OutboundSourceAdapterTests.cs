using System.Text.Json;
using Moq;
using MyApp.Contracts.Outbound;
using MyApp.Core.Outbound;
using Oracle.ManagedDataAccess.Client;
using Xunit;

namespace MyApp.Tests.Outbound;

public sealed record SamplePayload(int Id, string Name);

public class OutboundSourceAdapterTests
{
    private readonly JsonSerializerOptions _jsonOptions = new();

    [Fact]
    public async Task FetchPendingRawAsync_SerializesPayloadsWithOriginalKeys()
    {
        var innerMock = new Mock<IOutboundEventSource<SamplePayload>>();
        var items = new List<OutboundItem<SamplePayload>>
        {
            new("k1", new SamplePayload(1, "a")),
            new("k2", new SamplePayload(2, "b")),
        };
        innerMock.Setup(s => s.FetchPendingAsync(It.IsAny<CancellationToken>())).ReturnsAsync(items);

        var adapter = new OutboundSourceAdapter<SamplePayload>(innerMock.Object, _jsonOptions);
        var raw = await adapter.FetchPendingRawAsync(CancellationToken.None);

        Assert.Equal(2, raw.Count);
        Assert.Equal("k1", raw[0].IdempotencyKey);
        Assert.Equal("k2", raw[1].IdempotencyKey);
        Assert.Contains("\"Id\":1", raw[0].PayloadJson);
        Assert.Contains("\"Name\":\"a\"", raw[0].PayloadJson);
    }

    [Fact]
    public async Task GetSuccessParameters_DelegatesToInnerSourceUsingCachedItem()
    {
        var innerMock = new Mock<IOutboundEventSource<SamplePayload>>();
        var item = new OutboundItem<SamplePayload>("k1", new SamplePayload(1, "a"));
        innerMock.Setup(s => s.FetchPendingAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(new List<OutboundItem<SamplePayload>> { item });

        var expectedParams = new List<OracleProcedureParameter> { new("id", 1, OracleDbType.Int32) };
        innerMock.Setup(s => s.GetSuccessParameters(item)).Returns(expectedParams);

        var adapter = new OutboundSourceAdapter<SamplePayload>(innerMock.Object, _jsonOptions);
        await adapter.FetchPendingRawAsync(CancellationToken.None);

        var result = adapter.GetSuccessParameters("k1");

        Assert.Same(expectedParams, result);
    }

    [Fact]
    public async Task GetErrorParameters_DelegatesToInnerSourceWithGivenError()
    {
        var innerMock = new Mock<IOutboundEventSource<SamplePayload>>();
        var item = new OutboundItem<SamplePayload>("k1", new SamplePayload(1, "a"));
        innerMock.Setup(s => s.FetchPendingAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(new List<OutboundItem<SamplePayload>> { item });

        var error = new ProcessingError("boom");
        var expectedParams = new List<OracleProcedureParameter> { new("err", "boom", OracleDbType.Varchar2) };
        innerMock.Setup(s => s.GetErrorParameters(item, error)).Returns(expectedParams);

        var adapter = new OutboundSourceAdapter<SamplePayload>(innerMock.Object, _jsonOptions);
        await adapter.FetchPendingRawAsync(CancellationToken.None);

        var result = adapter.GetErrorParameters("k1", error);

        Assert.Same(expectedParams, result);
    }

    [Fact]
    public void GetSuccessParameters_ThrowsWhenKeyWasNeverFetched()
    {
        var innerMock = new Mock<IOutboundEventSource<SamplePayload>>();
        var adapter = new OutboundSourceAdapter<SamplePayload>(innerMock.Object, _jsonOptions);

        Assert.Throws<KeyNotFoundException>(() => adapter.GetSuccessParameters("unknown"));
    }

    [Fact]
    public async Task FetchPendingRawAsync_ClearsStaleCacheFromPreviousFetch()
    {
        var innerMock = new Mock<IOutboundEventSource<SamplePayload>>();
        var firstItem = new OutboundItem<SamplePayload>("k1", new SamplePayload(1, "a"));
        innerMock.SetupSequence(s => s.FetchPendingAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(new List<OutboundItem<SamplePayload>> { firstItem })
            .ReturnsAsync(new List<OutboundItem<SamplePayload>>());

        var adapter = new OutboundSourceAdapter<SamplePayload>(innerMock.Object, _jsonOptions);
        await adapter.FetchPendingRawAsync(CancellationToken.None); // содержит k1
        await adapter.FetchPendingRawAsync(CancellationToken.None); // пустая выборка -> кеш должен очиститься

        Assert.Throws<KeyNotFoundException>(() => adapter.GetSuccessParameters("k1"));
    }
}

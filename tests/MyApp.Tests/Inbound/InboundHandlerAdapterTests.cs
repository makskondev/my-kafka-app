using System.Text.Json;
using Moq;
using MyApp.Contracts.Inbound;
using MyApp.Core.Inbound;
using Xunit;

namespace MyApp.Tests.Inbound;

public sealed record SampleMessage(int Id, string Name);

public class InboundHandlerAdapterTests
{
    private readonly JsonSerializerOptions _jsonOptions = new() { PropertyNameCaseInsensitive = true };

    [Fact]
    public async Task HandleAsync_DeserializesJsonAndDelegatesToInnerHandler()
    {
        var innerMock = new Mock<IInboundEventHandler<SampleMessage>>();
        var adapter = new InboundHandlerAdapter<SampleMessage>(innerMock.Object, _jsonOptions);
        var context = new InboundEventContext("Sample", "sample-topic", "key-1");
        const string json = """{"Id":42,"Name":"foo"}""";

        await adapter.HandleAsync(json, context, CancellationToken.None);

        innerMock.Verify(h => h.HandleAsync(
                It.Is<SampleMessage>(m => m.Id == 42 && m.Name == "foo"),
                context,
                CancellationToken.None),
            Times.Once);
    }

    [Fact]
    public async Task HandleAsync_ThrowsWhenPayloadDeserializesToNull()
    {
        var innerMock = new Mock<IInboundEventHandler<SampleMessage>>();
        var adapter = new InboundHandlerAdapter<SampleMessage>(innerMock.Object, _jsonOptions);
        var context = new InboundEventContext("Sample", "sample-topic", null);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => adapter.HandleAsync("null", context, CancellationToken.None));

        Assert.Contains("Sample", ex.Message);
        innerMock.Verify(
            h => h.HandleAsync(It.IsAny<SampleMessage>(), It.IsAny<InboundEventContext>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task HandleAsync_PropagatesExceptionThrownByInnerHandler()
    {
        var innerMock = new Mock<IInboundEventHandler<SampleMessage>>();
        innerMock
            .Setup(h => h.HandleAsync(It.IsAny<SampleMessage>(), It.IsAny<InboundEventContext>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("db unavailable"));

        var adapter = new InboundHandlerAdapter<SampleMessage>(innerMock.Object, _jsonOptions);
        var context = new InboundEventContext("Sample", "sample-topic", "k");

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => adapter.HandleAsync("""{"Id":1,"Name":"x"}""", context, CancellationToken.None));
    }
}

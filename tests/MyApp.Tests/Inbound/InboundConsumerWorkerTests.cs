using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using MyApp.Contracts.Inbound;
using MyApp.Core.Configuration;
using MyApp.Core.Inbound;
using Xunit;

namespace MyApp.Tests.Inbound;

public class InboundConsumerWorkerTests
{
    private static IServiceScopeFactory BuildScopeFactory(string code, IInboundEventHandlerBase handler)
    {
        var services = new ServiceCollection();
        services.AddKeyedSingleton(code, handler);
        return services.BuildServiceProvider().GetRequiredService<IServiceScopeFactory>();
    }

    private static InboundConsumerWorker CreateWorker(InboundEventRegistration registration, IServiceScopeFactory scopeFactory)
        => new(
            registration,
            scopeFactory,
            Options.Create(new KafkaOptions { BootstrapServers = "localhost:9092" }),
            Mock.Of<IKafkaConsumerFactory>(), // не используется напрямую при вызове ProcessMessageAsync
            NullLogger<InboundConsumerWorker>.Instance);

    private static ConsumeResult<string, string> BuildConsumeResult(string key, string value, long offset = 5)
        => new()
        {
            Message = new Message<string, string> { Key = key, Value = value },
            TopicPartitionOffset = new TopicPartitionOffset("queue1", new Partition(0), new Offset(offset)),
        };

    [Fact]
    public async Task ProcessMessageAsync_OnSuccess_CommitsOffsetAndPassesCorrectContext()
    {
        var registration = new InboundEventRegistration("OrderCreated", "queue1", "group1");
        var handlerMock = new Mock<IInboundEventHandlerBase>();
        handlerMock
            .Setup(h => h.HandleAsync(It.IsAny<string>(), It.IsAny<InboundEventContext>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        var scopeFactory = BuildScopeFactory(registration.Code, handlerMock.Object);
        var worker = CreateWorker(registration, scopeFactory);

        var consumerMock = new Mock<IConsumer<string, string>>();
        var result = BuildConsumeResult("key-1", "{}");

        await worker.ProcessMessageAsync(consumerMock.Object, result, CancellationToken.None);

        handlerMock.Verify(h => h.HandleAsync(
                "{}",
                It.Is<InboundEventContext>(ctx => ctx.EventTypeCode == "OrderCreated" && ctx.Key == "key-1"),
                It.IsAny<CancellationToken>()),
            Times.Once);
        consumerMock.Verify(c => c.Commit(result), Times.Once);
    }

    [Fact]
    public async Task ProcessMessageAsync_WhenHandlerThrows_DoesNotCommitAndPropagatesException()
    {
        var registration = new InboundEventRegistration("OrderCreated", "queue1", "group1");
        var handlerMock = new Mock<IInboundEventHandlerBase>();
        handlerMock
            .Setup(h => h.HandleAsync(It.IsAny<string>(), It.IsAny<InboundEventContext>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("db unavailable"));

        var scopeFactory = BuildScopeFactory(registration.Code, handlerMock.Object);
        var worker = CreateWorker(registration, scopeFactory);

        var consumerMock = new Mock<IConsumer<string, string>>();
        var result = BuildConsumeResult("key-1", "{}");

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => worker.ProcessMessageAsync(consumerMock.Object, result, CancellationToken.None));

        consumerMock.Verify(c => c.Commit(It.IsAny<ConsumeResult<string, string>>()), Times.Never);
    }
}

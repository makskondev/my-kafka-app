using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using MyApp.Contracts.Outbound;
using MyApp.Core.Outbound;
using MyApp.Data;
using Oracle.ManagedDataAccess.Client;
using Xunit;

namespace MyApp.Tests.Outbound;

public class OutboundPollingWorkerTests
{
    private static IServiceScopeFactory BuildScopeFactory(string code, IOutboundEventSourceBase source)
    {
        var services = new ServiceCollection();
        services.AddKeyedSingleton(code, source);
        return services.BuildServiceProvider().GetRequiredService<IServiceScopeFactory>();
    }

    private static OutboundPollingWorker CreateWorker(
        OutboundEventRegistration registration,
        IServiceScopeFactory scopeFactory,
        IProducer<string, string> producer,
        IOracleProcedureInvoker invoker)
        => new(registration, scopeFactory, producer, invoker, NullLogger<OutboundPollingWorker>.Instance);

    private static OutboundEventRegistration DefaultRegistration() =>
        new("InvoiceReady", "V_PENDING_INVOICES", "invoices.ready.v1", "PKG.SUCCESS", "PKG.ERROR", 15);

    [Fact]
    public async Task PollOnceAsync_WhenNoPendingItems_DoesNotCallProducerOrInvoker()
    {
        var registration = DefaultRegistration();
        var sourceMock = new Mock<IOutboundEventSourceBase>();
        sourceMock.Setup(s => s.FetchPendingRawAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(new List<RawOutboundItem>());

        var producerMock = new Mock<IProducer<string, string>>();
        var invokerMock = new Mock<IOracleProcedureInvoker>();
        var scopeFactory = BuildScopeFactory(registration.Code, sourceMock.Object);
        var worker = CreateWorker(registration, scopeFactory, producerMock.Object, invokerMock.Object);

        await worker.PollOnceAsync(CancellationToken.None);

        producerMock.Verify(p => p.ProduceAsync(
            It.IsAny<string>(), It.IsAny<Message<string, string>>(), It.IsAny<CancellationToken>()), Times.Never);
        invokerMock.Verify(i => i.InvokeAsync(
            It.IsAny<string>(), It.IsAny<IReadOnlyList<OracleProcedureParameter>>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task ProcessItemAsync_WhenProduceSucceeds_CallsSuccessProcedureWithSourceParameters()
    {
        var registration = DefaultRegistration();
        var item = new RawOutboundItem("k1", "{\"invoiceId\":1}");

        var successParams = new List<OracleProcedureParameter> { new("invoiceId", 1, OracleDbType.Decimal) };
        var sourceMock = new Mock<IOutboundEventSourceBase>();
        sourceMock.Setup(s => s.GetSuccessParameters("k1")).Returns(successParams);

        var producerMock = new Mock<IProducer<string, string>>();
        producerMock
            .Setup(p => p.ProduceAsync(registration.TargetTopic, It.IsAny<Message<string, string>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DeliveryResult<string, string> { Status = PersistenceStatus.Persisted });

        var invokerMock = new Mock<IOracleProcedureInvoker>();
        var scopeFactory = BuildScopeFactory(registration.Code, sourceMock.Object);
        var worker = CreateWorker(registration, scopeFactory, producerMock.Object, invokerMock.Object);

        await worker.ProcessItemAsync(sourceMock.Object, item, CancellationToken.None);

        invokerMock.Verify(i => i.InvokeAsync("PKG.SUCCESS", successParams, It.IsAny<CancellationToken>()), Times.Once);
        invokerMock.Verify(i => i.InvokeAsync("PKG.ERROR", It.IsAny<IReadOnlyList<OracleProcedureParameter>>(), It.IsAny<CancellationToken>()), Times.Never);

        producerMock.Verify(p => p.ProduceAsync(
            registration.TargetTopic,
            It.Is<Message<string, string>>(m => m.Key == "k1" && m.Value == item.PayloadJson),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task ProcessItemAsync_WhenDeliveryStatusIsNotPersisted_CallsErrorProcedureWithStatusReason()
    {
        var registration = DefaultRegistration();
        var item = new RawOutboundItem("k1", "{}");

        ProcessingError? capturedError = null;
        var errorParams = new List<OracleProcedureParameter> { new("invoiceId", 1, OracleDbType.Decimal) };
        var sourceMock = new Mock<IOutboundEventSourceBase>();
        sourceMock
            .Setup(s => s.GetErrorParameters("k1", It.IsAny<ProcessingError>()))
            .Callback<string, ProcessingError>((_, err) => capturedError = err)
            .Returns(errorParams);

        var producerMock = new Mock<IProducer<string, string>>();
        producerMock
            .Setup(p => p.ProduceAsync(registration.TargetTopic, It.IsAny<Message<string, string>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DeliveryResult<string, string> { Status = PersistenceStatus.NotPersisted });

        var invokerMock = new Mock<IOracleProcedureInvoker>();
        var scopeFactory = BuildScopeFactory(registration.Code, sourceMock.Object);
        var worker = CreateWorker(registration, scopeFactory, producerMock.Object, invokerMock.Object);

        await worker.ProcessItemAsync(sourceMock.Object, item, CancellationToken.None);

        invokerMock.Verify(i => i.InvokeAsync("PKG.ERROR", errorParams, It.IsAny<CancellationToken>()), Times.Once);
        invokerMock.Verify(i => i.InvokeAsync("PKG.SUCCESS", It.IsAny<IReadOnlyList<OracleProcedureParameter>>(), It.IsAny<CancellationToken>()), Times.Never);
        Assert.NotNull(capturedError);
        Assert.Contains("NotPersisted", capturedError!.Reason);
    }

    [Fact]
    public async Task ProcessItemAsync_WhenProduceThrows_CallsErrorProcedureWithExceptionReason()
    {
        var registration = DefaultRegistration();
        var item = new RawOutboundItem("k1", "{}");

        ProcessingError? capturedError = null;
        var errorParams = new List<OracleProcedureParameter>();
        var sourceMock = new Mock<IOutboundEventSourceBase>();
        sourceMock
            .Setup(s => s.GetErrorParameters("k1", It.IsAny<ProcessingError>()))
            .Callback<string, ProcessingError>((_, err) => capturedError = err)
            .Returns(errorParams);

        var producerMock = new Mock<IProducer<string, string>>();
        producerMock
            .Setup(p => p.ProduceAsync(registration.TargetTopic, It.IsAny<Message<string, string>>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new KafkaException(new Error(ErrorCode.Local_Transport, "broker unreachable")));

        var invokerMock = new Mock<IOracleProcedureInvoker>();
        var scopeFactory = BuildScopeFactory(registration.Code, sourceMock.Object);
        var worker = CreateWorker(registration, scopeFactory, producerMock.Object, invokerMock.Object);

        await worker.ProcessItemAsync(sourceMock.Object, item, CancellationToken.None);

        invokerMock.Verify(i => i.InvokeAsync("PKG.ERROR", errorParams, It.IsAny<CancellationToken>()), Times.Once);
        Assert.NotNull(capturedError);
        Assert.NotNull(capturedError!.Exception);
        Assert.IsType<KafkaException>(capturedError.Exception);
    }

    [Fact]
    public async Task ProcessItemAsync_WhenProcedureInvokerThrows_SwallowsExceptionAndDoesNotPropagate()
    {
        var registration = DefaultRegistration();
        var item = new RawOutboundItem("k1", "{}");

        var sourceMock = new Mock<IOutboundEventSourceBase>();
        sourceMock.Setup(s => s.GetSuccessParameters("k1")).Returns(new List<OracleProcedureParameter>());

        var producerMock = new Mock<IProducer<string, string>>();
        producerMock
            .Setup(p => p.ProduceAsync(registration.TargetTopic, It.IsAny<Message<string, string>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DeliveryResult<string, string> { Status = PersistenceStatus.Persisted });

        var invokerMock = new Mock<IOracleProcedureInvoker>();
        invokerMock
            .Setup(i => i.InvokeAsync("PKG.SUCCESS", It.IsAny<IReadOnlyList<OracleProcedureParameter>>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("oracle down"));

        var scopeFactory = BuildScopeFactory(registration.Code, sourceMock.Object);
        var worker = CreateWorker(registration, scopeFactory, producerMock.Object, invokerMock.Object);

        // не должно бросать - строка "остаётся во view" и переобработается на следующем poll'е
        await worker.ProcessItemAsync(sourceMock.Object, item, CancellationToken.None);

        invokerMock.Verify(i => i.InvokeAsync("PKG.SUCCESS", It.IsAny<IReadOnlyList<OracleProcedureParameter>>(), It.IsAny<CancellationToken>()), Times.Once);
    }
}

using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using MyApp.Core.Configuration;
using MyApp.Core.Outbound;
using MyApp.Tests.TestDoubles;
using Xunit;

namespace MyApp.Tests.Outbound;

public class OutboundEventRegistrationExtensionsTests
{
    [Fact]
    public void AddOutboundEvents_ThrowsWhenSourceTypeNotFound()
    {
        var services = new ServiceCollection();
        var configs = new List<OutboundEventConfig>
        {
            new()
            {
                Code = "X",
                SourceView = "V_X",
                SuccessProcedure = "PKG.OK",
                ErrorProcedure = "PKG.ERR",
                TargetTopic = "x.v1",
                Source = "No.Such.Type, No.Such.Assembly",
            },
        };

        var ex = Assert.Throws<InvalidOperationException>(() => services.AddOutboundEvents(configs));
        Assert.Contains("not found", ex.Message);
    }

    [Fact]
    public void AddOutboundEvents_ThrowsWhenSourceDoesNotImplementContract()
    {
        var services = new ServiceCollection();
        var configs = new List<OutboundEventConfig>
        {
            new()
            {
                Code = "X",
                SourceView = "V_X",
                SuccessProcedure = "PKG.OK",
                ErrorProcedure = "PKG.ERR",
                TargetTopic = "x.v1",
                Source = typeof(NotAnOutboundSource).AssemblyQualifiedName!,
            },
        };

        var ex = Assert.Throws<InvalidOperationException>(() => services.AddOutboundEvents(configs));
        Assert.Contains("must implement", ex.Message);
    }

    [Fact]
    public void AddOutboundEvents_RegistersSourceResolvableByCodeViaKeyedDi()
    {
        var services = new ServiceCollection();
        services.AddSingleton(new JsonSerializerOptions());
        services.AddLogging();
        services.AddSingleton(Mock.Of<Confluent.Kafka.IProducer<string, string>>());
        services.AddSingleton(Mock.Of<MyApp.Data.IOracleProcedureInvoker>());

        var configs = new List<OutboundEventConfig>
        {
            new()
            {
                Code = "FakeOutbound",
                SourceView = "V_FAKE",
                SuccessProcedure = "PKG.OK",
                ErrorProcedure = "PKG.ERR",
                TargetTopic = "fake.v1",
                Source = typeof(FakeOutboundSource).AssemblyQualifiedName!,
            },
        };

        services.AddOutboundEvents(configs);
        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();

        var adapter = scope.ServiceProvider.GetRequiredKeyedService<IOutboundEventSourceBase>("FakeOutbound");

        Assert.NotNull(adapter);
    }
}

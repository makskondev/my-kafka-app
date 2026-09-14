using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Moq;
using MyApp.Core.Configuration;
using MyApp.Core.Inbound;
using MyApp.Tests.TestDoubles;
using Xunit;

namespace MyApp.Tests.Inbound;

public class InboundEventRegistrationExtensionsTests
{
    [Fact]
    public void AddInboundEvents_ThrowsWhenHandlerTypeNotFound()
    {
        var services = new ServiceCollection();
        var configs = new List<InboundEventConfig>
        {
            new()
            {
                Code = "X",
                SourceQueue = "q",
                ConsumerGroup = "g",
                Handler = "No.Such.Type, No.Such.Assembly",
            },
        };

        var ex = Assert.Throws<InvalidOperationException>(() => services.AddInboundEvents(configs));
        Assert.Contains("not found", ex.Message);
        Assert.Contains("X", ex.Message);
    }

    [Fact]
    public void AddInboundEvents_ThrowsWhenHandlerDoesNotImplementContract()
    {
        var services = new ServiceCollection();
        var configs = new List<InboundEventConfig>
        {
            new()
            {
                Code = "X",
                SourceQueue = "q",
                ConsumerGroup = "g",
                Handler = typeof(NotAnInboundHandler).AssemblyQualifiedName!,
            },
        };

        var ex = Assert.Throws<InvalidOperationException>(() => services.AddInboundEvents(configs));
        Assert.Contains("must implement", ex.Message);
    }

    [Fact]
    public void AddInboundEvents_RegistersHandlerResolvableByCodeViaKeyedDi()
    {
        var services = new ServiceCollection();
        services.AddSingleton(new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
        services.AddSingleton(Options.Create(new KafkaOptions { BootstrapServers = "localhost:9092" }));
        services.AddLogging();
        services.AddSingleton(Mock.Of<IKafkaConsumerFactory>());

        var configs = new List<InboundEventConfig>
        {
            new()
            {
                Code = "FakeInbound",
                SourceQueue = "q",
                ConsumerGroup = "g",
                Handler = typeof(FakeInboundHandler).AssemblyQualifiedName!,
            },
        };

        services.AddInboundEvents(configs);
        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();

        var adapter = scope.ServiceProvider.GetRequiredKeyedService<IInboundEventHandlerBase>("FakeInbound");

        Assert.NotNull(adapter);
    }
}

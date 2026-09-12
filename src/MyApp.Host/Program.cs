using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MyApp.Core.Configuration;
using MyApp.Core.Inbound;
using MyApp.Core.Outbound;
using MyApp.Data;

var builder = Host.CreateApplicationBuilder(args);

// --- Опции из конфигурации ---
builder.Services.Configure<KafkaOptions>(builder.Configuration.GetSection("Kafka"));
builder.Services.Configure<OracleOptions>(builder.Configuration.GetSection("Oracle"));

// --- Общая инфраструктура ---
builder.Services.AddSingleton<OracleConnectionFactory>();

builder.Services.AddSingleton(new JsonSerializerOptions
{
    PropertyNameCaseInsensitive = true,
});

builder.Services.AddSingleton<IProducer<string, string>>(sp =>
{
    var kafkaOptions = sp.GetRequiredService<IOptions<KafkaOptions>>().Value;
    return new ProducerBuilder<string, string>(new ProducerConfig
    {
        BootstrapServers = kafkaOptions.BootstrapServers,
        EnableIdempotence = true,
        Acks = Acks.All,
    }).Build();
});

// --- Регистрация событий из конфига ---
var inboundConfigs = builder.Configuration
    .GetSection("InboundEvents")
    .Get<List<InboundEventConfig>>() ?? [];

var outboundConfigs = builder.Configuration
    .GetSection("OutboundEvents")
    .Get<List<OutboundEventConfig>>() ?? [];

builder.Services.AddInboundEvents(inboundConfigs);
builder.Services.AddOutboundEvents(outboundConfigs);

var host = builder.Build();

// Fail fast: логируем итоговый состав зарегистрированных событий при старте
var logger = host.Services.GetRequiredService<ILogger<Program>>();
logger.LogInformation("Registered inbound events: {Codes}",
    string.Join(", ", inboundConfigs.Select(c => c.Code)));
logger.LogInformation("Registered outbound events: {Codes}",
    string.Join(", ", outboundConfigs.Select(c => c.Code)));

await host.RunAsync();

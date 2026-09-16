using System.Text.Json;
using Confluent.Kafka;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MyApp.Core.Configuration;
using MyApp.Core.Inbound;
using MyApp.Core.Kafka;
using MyApp.Core.Outbound;
using MyApp.Data;

var builder = WebApplication.CreateBuilder(args);

// --- Опции из конфигурации ---
builder.Services.Configure<KafkaOptions>(builder.Configuration.GetSection("Kafka"));
builder.Services.Configure<OracleOptions>(builder.Configuration.GetSection("Oracle"));

// --- Общая инфраструктура ---
builder.Services.AddSingleton<OracleConnectionFactory>();
builder.Services.AddSingleton<IOracleProcedureInvoker, OracleProcedureInvoker>();
builder.Services.AddSingleton<IKafkaConsumerFactory, KafkaConsumerFactory>();

builder.Services.AddSingleton(new JsonSerializerOptions
{
    PropertyNameCaseInsensitive = true,
});

builder.Services.AddSingleton<IProducer<string, string>>(sp =>
{
    var kafkaOptions = sp.GetRequiredService<IOptions<KafkaOptions>>().Value;
    var producerConfig = new ProducerConfig
    {
        BootstrapServers = kafkaOptions.BootstrapServers,
        EnableIdempotence = true,
        Acks = Acks.All,
    };
    producerConfig.ApplySecurity(kafkaOptions.Security); // тот же security-конфиг, что и у консьюмеров

    return new ProducerBuilder<string, string>(producerConfig).Build();
});

// --- Health checks: инфраструктурные (Oracle, Kafka) ---
// Health checks на конкретные события (по одному на каждый Code) регистрируются
// внутри AddInboundEvents/AddOutboundEvents ниже - см. MyApp.Core.
builder.Services.AddHealthChecks()
    .AddCheck<OracleHealthCheck>("oracle", tags: ["ready"])
    .AddCheck<KafkaProducerHealthCheck>("kafka-producer", tags: ["ready"]);

// --- Регистрация событий из конфига ---
var inboundConfigs = builder.Configuration
    .GetSection("InboundEvents")
    .Get<List<InboundEventConfig>>() ?? [];

var outboundConfigs = builder.Configuration
    .GetSection("OutboundEvents")
    .Get<List<OutboundEventConfig>>() ?? [];

builder.Services.AddInboundEvents(inboundConfigs);
builder.Services.AddOutboundEvents(outboundConfigs);

var app = builder.Build();

// Liveness: если процесс отвечает на HTTP - он жив. Никаких внешних зависимостей не
// проверяем здесь намеренно: сбой Oracle/Kafka не повод убивать и перезапускать под -
// это не исправит внешнюю проблему, а даст только рестарт-шторм. Для этого - readiness ниже.
app.MapHealthChecks("/health/live", new HealthCheckOptions
{
    Predicate = _ => false,
});

// Readiness: агрегирует все чеки с тегом "ready" - инфраструктурные (Oracle, Kafka)
// и по одному на каждое зарегистрированное inbound/outbound событие.
// Degraded трактуем как "не готов" (503), а не "готов, но есть нюанс" (200) -
// для readiness-пробы это осознанно строже дефолта ASP.NET Core.
app.MapHealthChecks("/health/ready", new HealthCheckOptions
{
    Predicate = check => check.Tags.Contains("ready"),
    ResponseWriter = WriteHealthReportAsync,
    ResultStatusCodes =
    {
        [HealthStatus.Healthy] = StatusCodes.Status200OK,
        [HealthStatus.Degraded] = StatusCodes.Status503ServiceUnavailable,
        [HealthStatus.Unhealthy] = StatusCodes.Status503ServiceUnavailable,
    },
});

// Fail fast: логируем итоговый состав зарегистрированных событий при старте
var logger = app.Services.GetRequiredService<ILogger<Program>>();
logger.LogInformation("Registered inbound events: {Codes}",
    string.Join(", ", inboundConfigs.Select(c => c.Code)));
logger.LogInformation("Registered outbound events: {Codes}",
    string.Join(", ", outboundConfigs.Select(c => c.Code)));

await app.RunAsync();

static async Task WriteHealthReportAsync(HttpContext context, HealthReport report)
{
    context.Response.ContentType = "application/json";

    var payload = new
    {
        status = report.Status.ToString(),
        totalDurationMs = report.TotalDuration.TotalMilliseconds,
        checks = report.Entries.Select(e => new
        {
            name = e.Key,
            status = e.Value.Status.ToString(),
            description = e.Value.Description,
            durationMs = e.Value.Duration.TotalMilliseconds,
        }),
    };

    await context.Response.WriteAsync(JsonSerializer.Serialize(payload));
}
